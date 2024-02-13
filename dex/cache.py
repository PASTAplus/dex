import contextlib
import functools
import logging
import lzma
import pathlib
import shutil
import sys
import tempfile
import threading
import time

import fasteners
import flask
import lxml.etree

import dex.db
import dex.exc
import dex.filesystem

try:
    import cPickle as pickle
except ImportError:
    import pickle


class NamedThreadLocks:
    def __init__(self):
        self.locks = {}
        self.my_lock = threading.RLock()
        self.es = contextlib.ExitStack()
        self.out = pathlib.Path('/tmp/out').open('a')

    @contextlib.contextmanager
    def __call__(self, name):
        with self.my_lock:
            self.locks.setdefault(name, threading.RLock())
        ts = time.time()
        self.p(f'{name}: waiting')
        with self.locks[name]:
            acq_ts = time.time() - ts
            self.p(f'{name}: acquired after {acq_ts:.2f}s')
            yield
        release_ts = time.time() - ts
        self.p(f'{name}: released after {release_ts - acq_ts : .2f}s')

    def p(self, *a, **kw):
        print(*a, **kw, file=self.out)


named_thread_locks = NamedThreadLocks()

threading_lock = threading.RLock()


LOCK_ROOT = pathlib.Path(tempfile.gettempdir(), "DEX-LOCK")
LOCK_ROOT.mkdir(0o755, parents=True, exist_ok=True)

log = logging.getLogger(__name__)


@contextlib.contextmanager
def lock(rid, key, obj_type):
    rid = rid or 'global'
    log.debug(f"Waiting to acquire lock: {rid}_{key}_{obj_type}")
    with named_thread_locks(f"{rid}_{key}_{obj_type}"):
        start_ts = time.time()
        with fasteners.InterProcessLock((LOCK_ROOT / f"{rid}_{key}_{obj_type}").as_posix()):
            log.debug(
                f"Acquired lock after {time.time() - start_ts : .02f}s: " f"{rid}_{key}_{obj_type}"
            )
            yield
        log.debug(f"Released lock: {rid}_{key}_{obj_type}")


def disk(key, obj_type):
    """Disk cache decorator.

    The first arg to the wrapped function must be `rid`.

    Calls to this method are serialized using locks for performance. In the future, we
    might want to add hardware detection to check how many concurrent disk read/write
    operations the server can do before performance starts going down. On machines with
    only a single HDD or SSD, having a single global lock may be optimal.

    The general strategy is that we use nested calls that each may be filled from the
    cache. E.g., the function to get the head of a CSV file calls the function to get
    the full CSV and extracts the head from it. Serving a single page triggers a series
    of such functions. The goal is to cause the first function that acquires the lock to
    prepare the underlying objects while the other functions first wait and then can use
    the already prepared objects. Without locking, we'd end up loading the full CSV file
    into memory in many concurrent processes.

    Also important to note is that reading multiple files at the same time from the same
    HDD causes disk thrashing and extremely bad performance. The situation is much
    better on SSDs, but the bandwidth to the disk can be saturated, so, while there may
    not be a large disadvantage to concurrent access as with HDDs, it's also not
    necessarily very beneficial.

    The locks also prevent attempts to read cached items while they're being written.

    Args:
        key:
        obj_type:
    """

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(rid, *args, **kwargs):
            with lock(rid, key, obj_type):
                if flask.current_app.config["DISK_CACHE_ENABLED"] and is_cached(rid, key, obj_type):
                    obj = read_from_cache(rid, key, obj_type)
                    log.debug(
                        f'Using cached object. key="{key}" obj_type="{obj_type}" '
                        f'class="{obj.__class__.__name__}" '
                        f'ram="{sys.getsizeof(obj, -1):,} bytes"'
                    )
                    return obj
                else:
                    log.debug(
                        f'Object is not cached. Generating it. key="{key}" obj_type="{obj_type}" '
                    )
                    # except dex.exc.CacheError as e:
                    #     log.debug(f'Error: {repr(e)}')
                    obj = fn(rid, *args, **kwargs)
                    save_to_cache(rid, key, obj_type, obj)
                    log.debug(
                        f'Caching new object, then returning it to client. '
                        f'key="{key}" obj_type="{obj_type}" '
                        f'class="{obj.__class__.__name__}" '
                        f'ram="{sys.getsizeof(obj, -1):,} bytes"'
                    )
                    return obj

        return wrapper

    return decorator


def is_cached(rid, key, obj_type):
    if not flask.current_app.config["DISK_CACHE_ENABLED"]:
        return False

    for is_compressed in (False, True):
        cache_path = _get_cache_path(rid, key, obj_type, is_compressed)
        if cache_path.exists():
            return True
    return False


def read_from_cache(rid, key, obj_type):
    return read_gen(rid, key, obj_type)


def read_gen(rid, key, obj_type):
    with open_file(rid, key, obj_type, for_write=False) as f:
        if obj_type in ("text", "csv", "html", "eml"):
            return f.read().decode("utf-8")
        elif obj_type in ("lxml", "etree"):
            return lxml.etree.parse(f)
        # elif obj_type in ('path',):
        #     cache_path, is_compressed = get_cache_path(rid, key, obj_type)
        else:
            log.debug(f'unpickling object type {obj_type}')
            return pickle.load(f)


def save_to_cache(rid, key, obj_type, obj):
    return save_gen(rid, key, obj_type, obj)


def save_gen(rid, key, obj_type, obj):
    with open_file(rid, key, obj_type, for_write=True) as f:
        if obj_type in ("text", "csv", "html", "eml", 'xml'):
            return f.write(obj.encode("utf-8"))
        elif obj_type in ("lxml", "etree"):
            with contextlib.suppress(LookupError, TypeError):
                if len(obj) == 1:
                    obj = obj[0]
            try:
                xml_str = lxml.etree.tostring(obj, with_tail=False)
                return f.write(xml_str)
            except TypeError:
                raise dex.exc.CacheError(
                    f'Unable to serialize object as an LXML or etree type. '
                    f'tag={get_tag_str(rid, key, obj_type)}'
                )
        else:
            log.debug(f'pickling object type {obj.__class__.__name__}')
            return pickle.dump(obj, f)


def get_tag_str(rid, key, obj_type):
    dist_url = dex.db.get_dist_url(rid)
    return f'dist_url="{dist_url}" key="{key}" type="{obj_type}"'


@contextlib.contextmanager
def open_file(rid, key, obj_type, for_write=False):
    cache_path, is_compressed = get_cache_path(rid, key, obj_type)
    if for_write:
        if flask.current_app.config["DISK_CACHE_ENABLED"] and cache_path.exists():
            raise dex.exc.CacheError(f"Cache file already exists: {cache_path.as_posix()}")
        cache_path.parent.mkdir(parents=True, exist_ok=True)
    else:
        if not cache_path.exists():
            raise dex.exc.CacheError(f"Cache file does not exist: {cache_path.as_posix()}")
    if is_compressed:
        with lzma.LZMAFile(
            filename=cache_path.as_posix(),
            mode="w" if for_write else "r",
            format=lzma.FORMAT_XZ,
            check=-1,
            preset=(lzma.PRESET_DEFAULT if for_write else None),
            filters=None,
        ) as f:
            yield f
    else:
        with cache_path.open("wb" if for_write else "rb") as f:
            yield f


def delete_cache_file(rid, key, obj_type):
    for is_compressed in (False, True):
        cache_path = _get_cache_path(rid, key, obj_type, is_compressed)
        if cache_path.exists():
            log.debug(f"Deleting cache file: {cache_path.as_posix()}")
            cache_path.unlink()


def flush_cache(rid):
    """Delete all cache files for the given rid"""
    cache_entity_root_path = _get_cache_entity_root_path(rid)
    shutil.rmtree(cache_entity_root_path.as_posix(), ignore_errors=True)


def get_cache_path(rid, key, obj_type):
    """
    Args:
        rid:
        key:
        obj_type:

    Returns:
        If path + '.xz' exists, returns (path + '.xz', is_compressed=True).
        Else, returns (path, is_compressed=False). path may or may not exist.
    """
    cache_path = _get_cache_path(rid, key, obj_type, is_compressed=True)
    if cache_path.exists():
        return cache_path, True
    return _get_cache_path(rid, key, obj_type, is_compressed=False), False


def _get_cache_path(rid, key, obj_type, is_compressed):
    return _get_cache_entity_root_path(rid) / f"{key}.{obj_type}{'.xz' if is_compressed else ''}"


def _get_cache_entity_root_path(rid):
    return pathlib.Path(
        flask.current_app.config['CACHE_ROOT_DIR'],
        dex.filesystem.get_safe_lossy_path_element(
            dex.db.get_dist_url(rid) if rid is not None else 'global'
        ),
    ).resolve()


class CacheFileDoesNotExist(Exception):
    pass

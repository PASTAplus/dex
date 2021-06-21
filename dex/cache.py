import contextlib
import functools
import logging
import lzma
import pathlib
import re
import sys
import tempfile
import threading
import time

import fasteners
import flask
import lxml.etree
import pandas as pd

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
                if flask.current_app.config["DISK_CACHE_ENABLED"] and is_cached(
                    rid, key, obj_type
                ):
                    return read_from_cache(rid, key, obj_type)
                else:
                    # except dex.exc.CacheError as e:
                    #     log.debug(f'Error: {repr(e)}')
                    obj = fn(rid, *args, **kwargs)
                    save_to_cache(rid, key, obj_type, obj)
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
    if obj_type == "df":
        return read_df(rid, key, obj_type)
    else:
        return read_gen(rid, key, obj_type)


def read_df(rid, key, obj_type):
    cache_path, is_compressed = get_cache_path(rid, key, obj_type)
    if is_compressed:
        raise AssertionError(
            "HDF (.m5) files use compression internally. "
            "Compression with external tools is not supported"
        )
    try:
        return pd.read_hdf(cache_path, key=re.sub("[^a-z0-9]", key, "_"))
    except AttributeError:
        # https://github.com/pandas-dev/pandas/issues/31199
        delete_cache_file(rid, key, obj_type)
        raise dex.exc.CacheError("Discarded corrupted HDF file")
    except FileNotFoundError:
        raise dex.exc.CacheError("File not in cache")


def read_gen(rid, key, obj_type):
    with open_file(rid, key, obj_type, for_write=False) as f:
        if obj_type in ("text", "csv", "html", "eml"):
            return f.read().decode("utf-8")
        elif obj_type in ("lxml", "etree"):
            return lxml.etree.parse(f)
        # elif obj_type in ('path',):
        #     cache_path, is_compressed = get_cache_path(rid, key, obj_type)
        else:
            return pickle.load(f)


def save_to_cache(rid, key, obj_type, obj):
    if obj_type == "df":
        return save_df(rid, key, obj_type, obj)
    else:
        return save_gen(rid, key, obj_type, obj)


def save_df(rid, key, obj_type, obj):
    cache_path, is_compressed = get_cache_path(rid, key, obj_type)
    if is_compressed:
        raise AssertionError(
            "HDF (.m5) files use compression internally. "
            "Compression with external tools is not supported"
        )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    obj.to_hdf(
        # cache_path.with_suffix('.tmp'),
        cache_path,
        key=re.sub("[^a-z0-9]", key, "_"),
        mode="w",
        complevel=6,
        complib="bzip2",
        # > Cannot store a category dtype in a HDF5 dataset that uses format="fixed".
        # Use format="table".
        format='table',
    )
    # os.rename(cache_path.with_suffix('.tmp'), cache_path)


def save_gen(rid, key, obj_type, obj):
    with open_file(rid, key, obj_type, for_write=True) as f:
        if obj_type in ("text", "csv", "html", "eml", 'xml'):
            return f.write(obj.encode("utf-8"))
        elif obj_type in ("lxml", "etree"):
            with contextlib.suppress(LookupError, TypeError):
                if len(obj) == 1:
                    obj = obj[0]
            try:
                xml_str = lxml.etree.tostring(obj)
                return f.write(xml_str)
            except TypeError:
                raise dex.exc.CacheError(
                    f'Unable to serialize object as an LXML or etree type. '
                    f'tag={get_tag_str(rid, key, obj_type)}'
                )
        else:
            log.debug(f'pickling object type {obj.__class__.__name__}')
            return pickle.dump(obj, f)


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
    return pathlib.Path(
        flask.current_app.config["CACHE_ROOT_DIR"],
        dex.filesystem.get_safe_lossy_path_element(
            dex.db.get_data_url(rid) if rid is not None else "global"
        ),
        f"{key}.{obj_type}{'.xz' if is_compressed else ''}",
    ).resolve()


class CacheFileDoesNotExist(Exception):
    pass

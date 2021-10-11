import datetime
import builtins
import collections
import contextlib
import io
import logging
import os
import pathlib
import pprint
import tempfile
import threading
import time
import types
import dateutil.parser
import dateutil
import fasteners
import flask
import flask.json
import lxml.etree
import pandas as pd
import pygments
import pygments.formatters
import pygments.lexers
from flask import current_app as app

import dex.db

log = logging.getLogger(__name__)


class Counter:
    def __init__(self):
        self.count_dict = collections.defaultdict(lambda: 0)
        self.last_msg_ts = None

    def __enter__(self):
        self.last_msg_ts = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        log.debug("-" * 100)
        self.print_counters(log.info)

    def count(self, id_str, key, detail_obj=None):
        self.count_dict[key] += 1
        if detail_obj is None:
            pass
        elif isinstance(detail_obj, str):
            self.p(f"{id_str}: {key}: {detail_obj}")
        else:
            self.pp(f'{id_str}: {key}:', detail_obj)
        if time.time() - self.last_msg_ts >= 1.0:
            self.last_msg_ts = time.time()
            self.print_counters(log.info)

    def print_counters(self, print_func):
        if not self.count_dict:
            print_func("No checks counted yet...")
            return
        print_func("Counters:")
        for k, v in sorted(self.count_dict.items()):
            print_func(f"  {v:>5,}: {k}")

    def pp(self, title_str, obj, print_func=None):
        self.p(title_str, print_func)
        [self.p(f'  {s}', print_func) for s in pprint.pformat(obj).splitlines(keepends=False)]

    def p(self, s, print_func=None):
        (print_func or log.debug)(s)


class CombinedLock:
    def __init__(self, root_path):
        self._root_path = pathlib.Path(root_path)
        self._thread_dict = {}

    @contextlib.contextmanager
    def lock(self, rid, key, obj_type, write=False):
        # log.debug(f'{self._root_path}, {lock_name}, {is_write}')
        lock_name = f'{rid}_{key}_{obj_type}'
        tp = f'{self._root_path / lock_name}_t'
        pp = f'{self._root_path / lock_name}_p'
        # print(f'tp = {tp}')
        # print(f'pp = {pp}')
        t = self._thread_dict.setdefault(tp, fasteners.ReaderWriterLock())
        p = self._thread_dict.setdefault(
            pp, fasteners.InterProcessReaderWriterLock(self._root_path / lock_name)
        )
        lock_fn = 'write_lock' if write else 'read_lock'
        es = contextlib.ExitStack()
        es.enter_context(getattr(t, lock_fn)())
        es.enter_context(getattr(p, lock_fn)())
        with es:
            try:
                yield es
            except Exception:
                pass


class Lock(object):
    """
    Thread locks work by having the threads access the same object in shared
    memory. So it's important that only one lock object object is created for
    each controlled resource.

    Process locks work by having the processes access a named external resource.
    So the lock works regardless of how many lock objects are created, as
    long as they reference the same named resource.
    """

    LOCK_METHOD_DICT = {
        'read': 'read_lock',
        'write': 'write_lock',
    }

    def __init__(self, name):
        self._name = name
        self._temp_dir = tempfile.TemporaryDirectory(prefix='locks')
        # Temp files are created under TemporaryDirectory, which is deleted
        # automatically, So delete is disabled on the individual files.
        self._lock_file = tempfile.NamedTemporaryFile(dir=self._temp_dir.name, delete=False)
        self._temp_root_path = pathlib.Path(self._temp_dir.name)
        self._lock_dict = {}
        self._thread_lock = threading.RLock()
        self._process_lock = fasteners.InterProcessLock(self._lock_file.name)

    @contextlib.contextmanager
    def lock_all(self):
        """Lock both threads and processes."""
        with self._thread_lock:
            with self._process_lock:
                yield self

    @contextlib.contextmanager
    def lock(self, rid, key, obj_type, write=False):
        """Lock name for read or write. Supports upgrade and downgrade of the lock in a
        nested context manager.
        """
        with self.lock_all():
            lock_name = f'{rid}_{key}_{obj_type}'
            if lock_name not in self._lock_dict:
                self._lock_dict[lock_name] = {
                    'thread': {
                        'lock': fasteners.ReaderWriterLock(),
                        'status': None,
                    },
                    'process': {
                        'lock': fasteners.InterProcessReaderWriterLock(
                            self._temp_root_path / lock_name
                        ),
                        'status': None,
                    },
                }

        type_str = 'write' if write else 'read'
        cm_list = [
            self._lock(lock_name, domain_str, type_str) for domain_str in ('thread', 'process')
        ]
        es = contextlib.ExitStack()
        [es.enter_context(cm) for cm in cm_list if cm is not None]
        with es:
            yield

    def _lock(self, lock_name, domain_str, type_str):
        lock_dict = self._lock_dict[lock_name]
        dom_dict = lock_dict[domain_str]
        if dom_dict['status'] == type_str:
            self._dbg(
                'Lock already acquired',
                lock_name=lock_name,
                domain_str=domain_str,
                type_str=type_str,
            )
            return None
        dom_dict['status'] = type_str
        lock_obj = dom_dict['lock']
        self._dbg('Waiting for lock', lock_name=lock_name, type_str=type_str)
        return getattr(lock_obj, self.LOCK_METHOD_DICT[type_str])()

    def _dbg(self, msg_str, **kv):
        log.debug(
            f"name:{self._name}, tid:{threading.get_native_id()}, pid={os.getpid()} - "
            f"{msg_str}: {' '.join([f'{k}={v}' for k, v in kv.items()])}"
        )


# Add SimpleNamespace as N to the global namespace.
# If PyCharm complains, add N to the list at:
# Settings > Inspections > Python > Unresolved references > Options > Ignore references
builtins.N = types.SimpleNamespace


def logpp(obj, msg=None, logger=log.debug, sort_keys=False):
    """pprint to a logger"""
    # if not logging.isEnabledFor(logging.DEBUG):
    #     return
    if lxml.etree.iselement(obj):
        obj_str = get_etree_as_pretty_printed_xml(obj)
    else:
        obj_str = pprint.pformat(obj, indent=2, width=200, sort_dicts=sort_keys)
    logger("-" * 100)
    if msg:
        logger(f'{msg}:')
    for line in obj_str.splitlines():
        logger(f'  {line}')
    logger("-" * 100)


# builtins.dd = logpp


# XML rendering


def get_etree_as_highlighted_html(el):
    """Return a (css, html) tuple"""
    xml_str = get_etree_as_pretty_printed_xml(el)
    html_formatter = pygments.formatters.HtmlFormatter(style=app.config['EML_STYLE_NAME'])
    return (
        pygments.highlight(xml_str, pygments.lexers.XmlLexer(), html_formatter),
        html_formatter.get_style_defs('.highlight'),
    )


def get_etree_as_pretty_printed_xml(el):
    """etree to pretty printed XML"""
    if not isinstance(el, list):
        el = [el]
    buf = io.BytesIO()
    for e in el:
        buf.write(lxml.etree.tostring(e, pretty_print=True))
    return buf.getvalue().decode('utf-8')


# JSON


class DatetimeEncoder(flask.json.JSONEncoder):
    def default(self, o):
        try:
            return super().default(o)
        except TypeError:
            if isinstance(o, datetime.date):
                return o.isoformat()
            return str(o)


class DatetimeDecoder(flask.json.JSONDecoder):
    # def decode(self, s, w=flask.json.decoder.WHITESPACE.match):
    import json

    def decode(self, s, w=json.decoder.WHITESPACE.match):
        try:
            return super().decode(s, w)
        except TypeError:
            try:
                return dateutil.parser.isoparse(s)
            except Exception:
                return s


def date_to_iso(**g_dict):
    return flask.json.loads(flask.json.dumps(g_dict))
    # return flask.json.loads(flask.json.dumps(g_dict, cls=DatetimeEncoder))


def json_enc(**g_dict):
    logpp(g_dict, 'g_dict', log.debug)
    j = flask.json.htmlsafe_dumps(g_dict)
    log.debug(j)
    return j


# Housekeeping


def wipe_cache():
    """Delete all cached objects from the filesystem and corresponding
    information from the database.
    """
    for p in (
        app.config['TMP_CACHE_ROOT'],
        app.config['CACHE_ROOT_DIR'],
    ):
        # p:pathlib.Path
        log.debug(f'Deleting dir tree: {p.as_posix()}')
        wipe_dir(p.resolve().absolute())

    log.debug(f'Deleting entities from database')
    dex.db.clear_entities()


def wipe_dir(p):
    """Delete everything inside the dir, but does not delete the dir itself.

    It seems like this can be a major footgun some time in the future, when a bug causes
    us to feed "/" to this, and it does its best to delete the whole server. What sort
    of check can we add to make it safer?
    """
    p: pathlib.Path
    log.debug(f'Entering dir: {p.as_posix()}')
    assert p.is_absolute()
    assert p.as_posix() not in ('', '/')
    for item in list(p.iterdir()):
        if item.is_dir():
            wipe_dir(item)
        else:
            item.unlink()


# def _is_file_uri(uri):
#     return uri.startswith('file://')


# def _is_url(uri):
#     return uri.startswith('http://') or uri.startswith('https://')


# def _uri_to_path(uri):
#     """Convert a file:// URI to a local path.
#
#     Args:
#         uri: file:// URI
#
#     Returns:
#         pathlib.Path()
#     """
#     uri_tup = urllib.parse.urlparse(uri)
#     p = pathlib.Path(
#         uri_tup.netloc,
#         urllib.request.url2pathname(urllib.parse.unquote(uri_tup.path)),
#     ).resolve()
#     if not p.exists():
#         raise dex.exc.CacheError(f"Invalid file URI: {uri}")
#     return p


def dump_full_dataframe(csv_df):
    if not log.isEnabledFor(logging.DEBUG):
        return
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        # log.debug('Final DF passed to Pandas Profiling:')
        log.debug('DataFrame:')
        log.debug(csv_df)
        log.debug('Info:')
        info_buf = io.StringIO()
        csv_df.info(verbose=True, buf=info_buf)
        log.debug(info_buf.getvalue())

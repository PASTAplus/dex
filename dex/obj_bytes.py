"""Provide an abstract source from which the bytes of PASTA CSV and EML objects can be
retrieved.

The CSV and EML bytes may be available at different source locations, to which bandwidth
and access methods differ (typically, local caches, filesystem storage, and the web,
each for either a production or test instance of the PASTA service. We also support
compressed objects, which variations in naming and compression algorithms. This module
abstracts those differences out by handling the initial search through the possible
sources, performing downloads and caching, and on the fly decompression as needed.

Possible source locations are checked in order of highest to lowest bandwidth to the
object bytes, so starts with a local filesystem cache, which is likely to be on locally
connected storage. Only objects that were downloaded and accessed recently are likely to
be available in the cache. The next step searches general PASTA package storage that is
available on the filesystem but may have lower bandwidth due to being accessed over a
local area network, such as NAS. Finally, PASTA web services are searched and, if found,
the object is downloaded and cached locally. For each search location, if both a
production and a test instance is available, the production instance is checked first,
then the test instance.

The eviction policy for the cache is Least Recently Used (LRU), which should keep
objects available locally during the initial processing. The access pattern for Dex is
that objects are accessed multiple times during the initial processing, after which
higher level caches are generated, and the source objects are not needed again.

Concurrency is handled by locking at the Row ID level, which prevents parallel processes
that require the same object from triggering multiple concurrent downloads or reads
of the same object. Instead, the download or read is triggered by the process that
first acquires the lock. While the single download occurs, subsequent processes are
blocked. When they in turn acquire the lock, high bandwidth access to the object is
already available.
"""
import logging
import pathlib
import shutil

import requests
from flask import current_app as app

import dex.db
import dex.exc
import dex.filesystem
import dex.pasta

log = logging.getLogger(__name__)

def open_csv(rid):
    """Given a PASTA entity tuple for a CSV in the form of a Row ID, return a stream
    of object bytes.

    The returned value is always a seekable stream holding the bytes of the CSV,
    regardless of where the bytes are located, accessed, decoded or decompressed.

    If the object bytes cannot be found, raises `dex.exc.CacheError`.
    """
    entity_tup = dex.db.get_entity(rid)
    log.debug(f'Resolving object bytes for CSV entity: {entity_tup}')
    return _open_obj(entity_tup, is_eml=False)


def open_eml(rid):
    """Given a PASTA entity tuple for a CSV in the form of a Row ID, return a stream
    of object bytes.

    The returned value is always a seekable stream holding the bytes of the CSV,
    regardless of where the bytes are located, how they are accessed, decoded or
    decompressed..

    If the object bytes cannot be found, raises `dex.exc.CacheError`.
    """
    entity_tup = dex.db.get_entity(rid)
    log.debug(f'Resolving object bytes for EML entity: {entity_tup}')
    return _open_obj(entity_tup, is_eml=True)


def _open_obj_with_alternate(entity_tup, is_eml):
    """First search the production or test environment designated by the base_url member
    of the entity_tup. If the object is not found there, repeat the search on the
    alternate environment. So, if the search started on production, then search test and
    vice versa.
    """
    obj = _open_obj(entity_tup, is_eml)
    if obj:
        return obj
    entity_tup.base_url = dex.pasta.get_other_base_url(entity_tup.base_url)
    obj = _open_obj(entity_tup, is_eml)
    if obj:
        return obj

    raise dex.exc.CacheError(f"Cannot get bytes for object: {dex.pasta.get_data_path(entity_tup)}")

def _open_obj(entity_tup, is_eml):
    """Handle initial retrieval and temporary caching of the source CSV and EML docs
    that can be processed by Dex.
    """
    obj = _open_obj_from_filesystem_cache(entity_tup, is_eml)
    if obj:
        return obj

    obj = _open_obj_from_filesystem_store(entity_tup, is_eml)
    if obj:
        return obj

    obj = _open_obj_from_pasta_service(entity_tup, is_eml)
    if obj:
        return obj



def _open_obj_from_filesystem_cache(entity_tup, is_eml):
    # log.info('Checking filesystem cache...')
    pass


def _open_obj_from_filesystem_store(entity_tup, is_eml):
    log.info('Checking filesystem package store...')
    if is_eml:
        return _get_eml_path(entity_tup)
    else:
        return _get_csv_path(entity_tup)


def _open_obj_from_pasta_service(entity_tup, is_eml):
    # log.info('Checking PASTA web service')
    pass


def _get_csv_path(entity_tup):
    data_path = dex.pasta.get_data_path(entity_tup)
    return _get_existing_path(data_path)


def _get_eml_path(entity_tup):
    eml_path = dex.pasta.get_eml_path(entity_tup)
    return _get_existing_path(eml_path)


def _get_existing_path(obj_path):
    log.info(f'Looking for object bytes at: {obj_path.as_posix()}')
    if obj_path.exists() and obj_path.stat().st_size:
        log.info('-> Found!')
        return obj_path
    log.info('-> Invalid path')


# Cache of downloaded objects


def _download_through_cache(obj_url):
    file_name = dex.filesystem.get_safe_reversible_path_element(obj_url)
    obj_path = app.config["TMP_CACHE_ROOT"] / file_name
    try:
        return obj_path.open('rb')
    except OSError:
        return _download_to_cache(obj_path, obj_url)


def _download_to_cache(obj_path, obj_url):
    """Download object and return opened stream"""
    assert isinstance(obj_path, pathlib.Path)

    obj_path.parent.mkdir(0o755, parents=True, exist_ok=True)

    _limit_cache_size()

    with obj_path.with_suffix('tmp').open('wb') as f:
        with requests.get(obj_url, stream=True) as r:
            r.raise_for_status()
            # This is a high performance way of copying a stream.
            shutil.copyfileobj(r.raw, f)

    if not r.ok:
        raise dex.exc.CacheError(f"Unable to download: {obj_url}")

    obj_path.with_suffix('tmp').rename(obj_path)

    return obj_path.open('rb')


def _limit_cache_size():
    """Delete the oldest cached file(s) if number of cached files have exceeded the
    limit."""
    # TODO: Should use combined size, not number of files
    path_list = list(app.config['TMP_CACHE_ROOT'].iterdir())
    while len(path_list) > app.config['TMP_CACHE_LIMIT']:
        oldest_path = min(path_list, key=lambda p: p.stat().st_mtime)
        oldest_path.unlink()
        path_list.remove(oldest_path)


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

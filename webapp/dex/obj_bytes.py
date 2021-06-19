"""Provide an abstract source from which the bytes of PASTA CSV and EML objects can be
retrieved.

The CSV and EML bytes may be available at different source locations, to which bandwidth
and access methods differ (typically, local caches, filesystem storage, and the web,
each for either a production or test instance of the PASTA service. This module
abstracts those differences out by handling the initial search through the possible
sources, and performing downloads and caching as required.

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
import pathlib
import urllib.parse
import urllib.request

from flask import current_app as app

import db
import dex.exc
import dex.pasta

CACHE_LIMIT = 10


def open_csv(rid):
    """Given a PASTA entity tuple for a CSV in the form of a row ID, return an open
    file-like object holding the bytes of the CSV.
    """
    return _open_obj(rid, is_eml=False)

def open_eml(rid):
    """Given a PASTA entity tuple for a CSV in the form of a row ID, return an open
    file-like object holding the bytes of the CSV.
    """
    return _open_obj(rid, is_eml=True)


def _open_obj(rid, is_eml):


Handle initial retrieval and temporary caching of the source CSV and EML docs that
can be processed by Dex.
"""

    
    Args:
        rid: 

    Returns:

    """


def get_data_path_by_row_id(rid):
    entity_tup = db.get_entity(rid)
    data_url = dex.pasta.get_data_url(entity_tup)
    data_path = dex.pasta.get_data_path(entity_tup)
    return _get_path(data_path, data_url)


def get_eml_path_by_row_id(rid):
    entity_tup = db.get_entity(rid)
    eml_url = dex.pasta.get_eml_url(entity_tup)
    eml_path = dex.pasta.get_eml_path(entity_tup)
    return _get_path(eml_path, eml_url)


def _get_path(obj_path, obj_url):
    if obj_path.exists() and obj_path.stat().st_size:
        return obj_path
    return _download_to_cache(obj_url)


def _download_to_cache(obj_url):
    file_name = urllib.parse.quote(obj_url, safe='')
    p = app.config['TMP_CACHE_ROOT'] / file_name
    if p.exists():
        return p
    p.parent.mkdir(0o755, parents=True, exist_ok=True)
    _limit_cache_size()
    with p.open('wb') as f:
        dex.pasta.download_data_entity(f, obj_url)
    if not p.exists():
        raise dex.exc.CacheError(f"Unable to download: {obj_url}")
    return p


def _limit_cache_size():
    """Delete the oldest cached file(s) if number of cached files have exceeded the
    limit."""
    path_list = list(app.config['TMP_CACHE_ROOT'].iterdir())
    while len(path_list) > app.config['TMP_CACHE_LIMIT']:
        oldest_path = min(path_list, key=lambda p: p.stat().st_mtime)
        oldest_path.unlink()
        path_list.remove(oldest_path)


def _is_file_uri(uri):
    return uri.startswith('file://')


def _is_url(uri):
    return uri.startswith('http://') or uri.startswith('https://')


def _uri_to_path(uri):
    """Convert a file:// URI to a local path.

    Args:
        uri: file:// URI

    Returns:
        pathlib.Path()
    """
    uri_tup = urllib.parse.urlparse(uri)
    p = pathlib.Path(
        uri_tup.netloc,
        urllib.request.url2pathname(urllib.parse.unquote(uri_tup.path)),
    ).resolve()
    if not p.exists():
        raise dex.exc.CacheError(f"Invalid file URI: {uri}")
    return p

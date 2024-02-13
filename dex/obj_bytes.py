"""Provide an abstract source from which the bytes of PASTA CSV and EML objects can be
read.

The CSV and EML bytes may be available at different source locations, to which bandwidth
and access methods differ (typically, local temporary filesystem cache, local filesystem
package store, and the PASTA web service), each for either a production or test instance
of the PASTA service. We also support compressed objects, which variations in naming and
compression algorithms. This module abstracts those differences out by handling the
initial search through the possible sources, performing downloads and caching, and on
the fly decompression as needed.

Possible source locations are checked in order of assumed highest to lowest estimated
bandwidth to the object bytes, as follows:

- Starts with checking if the object is in the local temporary filesystem cache, which
should be on locally connected storage. Only the most recently downloaded objects are
available on this cache. The eviction policy for the cache is Least Recently Used (LRU),
which should keep objects available locally during the initial processing.

- Next, checks if the object is the local filesystem PASTA package store, if one is
available and the packages stored there are for the PASTA environment that was
requested. This storage may have lower bandwidth due to being accessed over a local area
network, such as a NAS.

- Finally, the PASTA web services for the environment that was requested is searched. If
the object is found there, it is downloaded and cached in the local temporary filesystem
cache, where it is found in the first step of any subsequent requests.

- If the object cannot be found, raises dex.exc.CacheError().

The access pattern for DeX is that objects are accessed multiple times during the
initial processing, after which higher level caches are generated, and the source
objects are not needed again.

Concurrency is handled by locking at the Row ID level, which prevents parallel processes
that require the same object from triggering multiple concurrent downloads or reads of
the same object. Instead, the download or read is triggered by the process that first
acquires the lock. While the single download occurs, subsequent processes are blocked.
When they in turn acquire the lock, high bandwidth access to the object is already
available.
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


def open_eml(rid):
    """Given a Row ID, return a pathlib.Path to the EML file.

    The Path is always a valid path to a local file. If the bytes are not already
    available locally, they are downloaded to a temporary cache, and the return Path
    points to that location.

    If the object bytes cannot be found, raises `dex.exc.CacheError`.
    """
    entity_tup = dex.db.get_entity(rid)
    _log(entity_tup.dist_url, 'Resolving location for metadata object bytes')
    return _open_obj(entity_tup.dist_url, entity_tup.meta_url, is_eml=True)


def open_csv(rid):
    """Like open_eml(), only for a data (CSV) object instead of a metadata (EML) object."""
    entity_tup = dex.db.get_entity(rid)
    _log(entity_tup.dist_url, f'Resolving location for data object bytes')
    return _open_obj(entity_tup.dist_url, entity_tup.data_url, is_eml=False)


def _open_obj(dist_url, obj_url, is_eml):
    """Search for object in locations going from fastest to slowest access to the object
    bytes.
    """
    if not obj_url:
        obj_path = _local_sample_store(dist_url, is_eml)
        if obj_path:
            return obj_path
    else:
        obj_path = _filesystem_cache(dist_url, obj_url)
        if obj_path:
            return obj_path
        obj_path = _local_package_store(dist_url, obj_url, is_eml)
        if obj_path:
            return obj_path
        obj_path = _remote_url(dist_url, obj_url)
        if obj_path:
            return obj_path
    raise dex.exc.CacheError(
        f'Cannot find bytes for object. dist_url, obj_url="{dist_url, obj_url}""'
    )


def _filesystem_cache(dist_url, obj_url):
    _log(obj_url, 'Checking filesystem cache')
    obj_path = _get_cache_path(dist_url, obj_url)
    if _is_valid(obj_path):
        return obj_path


def _local_package_store(dist_url, obj_url, is_eml):
    if not dex.pasta.get_portal_base(dist_url) != app.config['PASTA_BASE_URL']:
        _log(
            obj_url,
            'Skipped local package store lookup (package is in another environment)',
        )
        return
    _log(obj_url, 'Checking local package store')
    fn = dex.pasta.get_local_package_meta_path if is_eml else dex.pasta.get_local_package_data_path
    obj_path = fn(dist_url)
    if _is_valid(obj_path):
        return obj_path


def _local_sample_store(dist_url, is_eml):
    _log(dist_url, 'Checking local sample store')
    fn = dex.pasta.get_local_sample_meta_path if is_eml else dex.pasta.get_local_sample_data_path
    obj_path = fn(dist_url)
    if _is_valid(obj_path):
        return obj_path


def _remote_url(dist_url, obj_url):
    _log(obj_url, 'Downloading object bytes')
    obj_path = _get_cache_path(dist_url, obj_url)
    _limit_cache_size()
    obj_path.parent.mkdir(0o755, parents=True, exist_ok=True)
    obj_tmp_path = _get_cache_tmp_path(dist_url, obj_url)
    with obj_tmp_path.open('wb') as f:
        with requests.get(obj_url, stream=True) as r:
            r.raise_for_status()
            # This is a high performance way of copying a stream.
            shutil.copyfileobj(r.raw, f)
    if not r.ok:
        msg_str = f'Failed to download object bytes: {r.status_code} {r.reason}'
        _log(obj_url, msg_str)
        raise dex.exc.CacheError(msg_str)
    obj_path.unlink(missing_ok=True)
    obj_tmp_path.rename(obj_path)
    return obj_path


def _limit_cache_size():
    """Delete the oldest cached file(s) if number of cached files has exceeded the
    limit.
    """
    path_list = list(app.config['TMP_CACHE_ROOT'].iterdir())
    while len(path_list) > app.config['TMP_CACHE_LIMIT']:
        oldest_path = min(path_list, key=lambda p: p.stat().st_mtime)
        oldest_path.unlink()
        path_list.remove(oldest_path)


def _get_cache_path(dist_url, obj_url):
    return app.config["TMP_CACHE_ROOT"] / dex.filesystem.get_safe_lossy_path(dist_url, obj_url)


def _get_cache_tmp_path(dist_url, obj_url):
    return pathlib.Path(_get_cache_path(dist_url, obj_url).as_posix() + '.tmp')


def _is_valid(obj_path):
    is_found = obj_path.exists() and obj_path.stat().st_size
    if is_found:
        _log(obj_path.as_posix(), 'Found object bytes')
    return is_found


def _log(obj_url, msg_str):
    log.debug(f'{msg_str}: {obj_url}')

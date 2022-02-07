"""Provide an abstract source from which the bytes of PASTA CSV and EML objects can be
read.

The CSV and EML bytes may be available at different source locations, to which bandwidth
and access methods differ (typically, local temporary filesystem cache, local filesystem
package store, and the PASTA web service, each for either a production or test instance
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


def open_csv(rid):
    """Given a PASTA entity tuple for a CSV in the form of a Row ID, return a stream
    of object bytes.

    The returned value is always a seekable stream holding the bytes of the CSV,
    regardless of where the bytes are located, accessed, decoded or decompressed.

    If the object bytes cannot be found, raises `dex.exc.CacheError`.
    """
    entity_tup = dex.db.get_entity(rid)
    _log(entity_tup, False, f'Resolving location for object bytes')
    return _open_obj(entity_tup, is_eml=False)


def open_eml(rid):
    """Given a PASTA entity tuple for a CSV in the form of a Row ID, return a stream
    of object bytes.

    The returned value is always a seekable stream holding the bytes of the CSV,
    regardless of where the bytes are located, how they are accessed, decoded or
    decompressed.

    If the object bytes cannot be found, raises `dex.exc.CacheError`.
    """
    entity_tup = dex.db.get_entity(rid)
    _log(entity_tup, True, 'Resolving location for object bytes')
    return _open_obj(entity_tup, is_eml=True)


def _open_obj(entity_tup, is_eml):
    """Search for object in locations going from fastest to slowest access to the object
    bytes.
    """
    obj = _filesystem_cache(entity_tup, is_eml)
    if obj:
        return obj
    obj = _local_package_store(entity_tup, is_eml)
    if obj:
        return obj
    obj = _pasta_service(entity_tup, is_eml)
    if obj:
        return obj
    raise dex.exc.CacheError(
        f'Cannot find bytes for object. entity_tup="{entity_tup}" is_eml="{is_eml}"'
    )


def _filesystem_cache(entity_tup, is_eml):
    _log(entity_tup, is_eml, 'Checking filesystem cache')
    return _get_existing_path(_get_cache_path(entity_tup, is_eml))


def _local_package_store(entity_tup, is_eml):
    if entity_tup.base_url != app.config['PASTA_BASE_URL']:
        _log(entity_tup, is_eml, 'Skipped local file lookup')
        return
    _log(entity_tup, is_eml, 'Checking filesystem package store')
    return _get_existing_path(_get_package_store_path(entity_tup, is_eml))


def _pasta_service(entity_tup, is_eml):
    _log(entity_tup, is_eml, 'Checking PASTA web service')
    return _download_through_cache(entity_tup, is_eml)


def _download_through_cache(entity_tup, is_eml):
    """Download object and return opened stream."""
    _log(entity_tup, is_eml, 'Downloading from PASTA service')

    obj_path = _get_cache_path(entity_tup, is_eml)
    if _exists_and_non_zero(obj_path):
        return obj_path

    _limit_cache_size()

    obj_tmp_path = _get_cache_path(entity_tup, is_eml)
    obj_tmp_path.parent.mkdir(0o755, parents=True, exist_ok=True)
    obj_url = _get_obj_url(entity_tup, is_eml)

    with obj_tmp_path.open('wb') as f:
        with requests.get(obj_url, stream=True) as r:
            r.raise_for_status()
            # This is a high performance way of copying a stream.
            shutil.copyfileobj(r.raw, f)

    if not r.ok:
        raise dex.exc.CacheError(f"Unable to download: {obj_url}")

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


def _log(entity_tup, is_eml, msg_str):
    log.debug(f'{entity_tup.data_url} {"EML" if is_eml else "CSV"}: {msg_str}')


def _get_obj_url(entity_tup, is_eml):
    if is_eml:
        return dex.pasta.get_eml_url(entity_tup)
    else:
        return dex.pasta.get_data_url(entity_tup)


def _get_cache_path(entity_tup, is_eml):
    return app.config["TMP_CACHE_ROOT"] / pathlib.Path(
        dex.filesystem.get_safe_reversible_path_element(
            entity_tup.data_url + ('.eml' if is_eml else '.csv')
        )
    )


def _get_cache_tmp_path(entity_tup, is_eml):
    return _get_cache_path(entity_tup, is_eml) + '.csv'


def _get_package_store_path(entity_tup, is_eml):
    if is_eml:
        return dex.pasta.get_eml_path(entity_tup)
    else:
        return dex.pasta.get_data_path(entity_tup)


def _get_existing_path(obj_path):
    if _exists_and_non_zero(obj_path):
        return obj_path


def _exists_and_non_zero(obj_path):
    is_found = obj_path.exists() and obj_path.stat().st_size
    log.debug(f'{"Found object bytes" if is_found else "Invalid path"}: {obj_path.as_posix()}')
    return is_found

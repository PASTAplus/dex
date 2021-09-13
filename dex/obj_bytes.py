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
the object is downloaded and cached locally. Each PASTA service has a production and a
test environment. Both environments are searched, with the request from which the
request originated, searched first.

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
    decompressed.

    If the object bytes cannot be found, raises `dex.exc.CacheError`.
    """
    entity_tup = dex.db.get_entity(rid)
    log.debug(f'Resolving object bytes for EML entity:')
    log.debug(f'entity_tup="{entity_tup}" PASTA_BASE_URL="{app.config["PASTA_BASE_URL"]}"')
    return _open_obj(entity_tup, is_eml=True)


def _open_obj(entity_tup, is_eml):
    """Search for object in locations going from fastest to slowest access to the object bytes."""
    # obj = _open_obj_from_filesystem_cache(entity_tup, is_eml)
    # if obj:
    #     log.debug(f'_open_obj_from_filesystem_cache ret: {obj}')
    #     return obj
    if entity_tup.base_url == app.config['PASTA_BASE_URL']:
        log.info(f'Performing local file lookup: Local files belong to this PASTA environment.')
        obj = _open_obj_from_filesystem_store(entity_tup, is_eml)
        if obj:
            log.debug(f'_open_obj_from_filesystem_store ret: {obj}')
            return obj
    else:
        log.debug(f'Skipped local file lookup: Local files belong to another PASTA environment.')
    obj = _open_obj_from_pasta_service(entity_tup, is_eml)
    if obj:
        log.debug(f'_open_obj_from_pasta_service ret: {obj}')
        return obj
    raise dex.exc.CacheError(
        f'Cannot find bytes for object. entity_tup="{entity_tup}" is_eml="{is_eml}"'
    )


def _open_obj_from_filesystem_cache(entity_tup, is_eml):
    log.debug(f'Checking filesystem cache. entity_tup="{entity_tup}" is_eml="{is_eml}"')
    try:
        # return _get_cache_path(entity_tup, is_eml).open('rb')
        return _get_cache_path(entity_tup, is_eml)
    except OSError:
        pass


def _open_obj_from_filesystem_store(entity_tup, is_eml):
    log.debug(f'Checking filesystem package store. entity_tup="{entity_tup}" is_eml="{is_eml}"')
    if is_eml:
        return _get_eml_path(entity_tup)
    else:
        return _get_csv_path(entity_tup)


def _open_obj_from_pasta_service(entity_tup, is_eml):
    log.debug(f'Checking PASTA web service. entity_tup="{entity_tup}" is_eml="{is_eml}"')
    return _download_through_cache(entity_tup, is_eml)


def _get_csv_path(entity_tup):
    data_path = dex.pasta.get_data_path(entity_tup)
    return _get_existing_path(data_path)


def _get_eml_path(entity_tup):
    eml_path = dex.pasta.get_eml_path(entity_tup)
    return _get_existing_path(eml_path)


def _get_existing_path(obj_path):
    log.debug(f'Looking for object bytes at: {obj_path.as_posix()}')
    if obj_path.exists() and obj_path.stat().st_size:
        log.debug('-> Found!')
        return obj_path
    log.debug('-> Invalid path')


# Cache of downloaded objects


def _open_from_filesystem_cache(entity_tup, is_eml):
    log.debug(f'Checking local filesystem cache. entity_tup={entity_tup} is_eml="{is_eml}"')
    obj_path = _get_cache_path(entity_tup, is_eml)
    try:
        return obj_path  # .open('rb')
    except OSError:
        pass


def _download_through_cache(entity_tup, is_eml):
    """Download object and return opened stream"""
    log.debug(f'Downloading from PASTA service. entity_tup="{entity_tup}" is_eml="{is_eml}"')
    obj_path = _get_cache_path(entity_tup, is_eml)
    # assert not obj_path.exists()
    obj_path.parent.mkdir(0o755, parents=True, exist_ok=True)

    _limit_cache_size()

    obj_url = _get_obj_url(entity_tup, is_eml)

    with obj_path.with_suffix('.tmp').open('wb') as f:
        with requests.get(obj_url, stream=True) as r:
            r.raise_for_status()
            # This is a high performance way of copying a stream.
            shutil.copyfileobj(r.raw, f)

    if not r.ok:
        raise dex.exc.CacheError(f"Unable to download: {obj_url}")

    obj_path.with_suffix('.tmp').rename(obj_path)

    return obj_path
    # return obj_path.open('rb')


def _limit_cache_size():
    """Delete the oldest cached file(s) if number of cached files have exceeded the
    limit."""
    # TODO: Should use combined size, not number of files
    path_list = list(app.config['TMP_CACHE_ROOT'].iterdir())
    while len(path_list) > app.config['TMP_CACHE_LIMIT']:
        oldest_path = min(path_list, key=lambda p: p.stat().st_mtime)
        oldest_path.unlink()
        path_list.remove(oldest_path)


def _get_cache_path(entity_tup, is_eml):
    return app.config["TMP_CACHE_ROOT"] / pathlib.Path(
        dex.filesystem.get_safe_reversible_path_element(
            entity_tup.data_url + ('.eml' if is_eml else '.csv') + '.tmp'
        )
    )
    # return (
    #    app.config["TMP_CACHE_ROOT"]
    #    / pathlib.Path(
    #        escape(entity_tup.data_url),
    #    ).with_suffix('.eml' if is_eml else '.csv')
    # )


def _get_obj_url(entity_tup, is_eml):
    if is_eml:
        return dex.pasta.get_eml_url(entity_tup)
    else:
        return dex.pasta.get_data_url(entity_tup)

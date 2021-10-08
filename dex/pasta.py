#!/usr/bin/env python

"""Utilities for interacting with the PASTA Restful API"""

import collections
import logging
import pathlib
import re
import shutil
import types

import requests
from flask import current_app as app

import dex.exc
import dex.util

DATA_URL_RX = re.compile(
    r"""
    (?P<base_url>.*/package)
    /data/eml/
    (?P<scope_str>[^/]+)/
    (?P<id_str>\d+)/
    (?P<ver_str>\d+)/
    (?P<entity_str>[0-9a-fA-F]{32,})
    $
    """,
    re.VERBOSE,
)

DATA_PATH_RX = re.compile(
    r"""
      (?P<base_url>.*)/
      (?P<scope_str>[^.]*)\.
      (?P<id_str>\d+)\.
      (?P<ver_str>\d+)/
      (?P<entity_str>[0-9a-fA-F]{32,})
      $
    """,
    re.VERBOSE,
)

PASTA_PORTAL_DICT = {
    # https://portal-d.edirepository.org/nis/home.jsp
    # https://portal.edirepository.org/nis/mapbrowse?scope=knb-lter-luq&identifier=148&revision=1213903
    'https://pasta.edirepository.org/package': 'https://portal.edirepository.org/nis',
    'https://pasta-d.edirepository.org/package': 'https://portal-d.edirepository.org/nis',
    'https://pasta.lternet.edu/package': 'https://portal.edirepository.org/nis',
    'https://pasta-d.lternet.edu/package': 'https://portal-d.edirepository.org/nis',
    'https://pasta-s.lternet.edu/package': 'https://portal-s.edirepository.org/nis',
    # Dev
    'https://localhost/package': 'https://portal-d.localhost/nis',
}

# DATA_PATH_RX = re.compile(
#     r"""(?P<entity_str>.*)""",
#     re.VERBOSE,
# )

log = logging.getLogger(__name__)

# TODO: Keeping the base_url in the EntityTup ties the tuple to a location. See how it
# would work out to keep the tuple more agnostic by leaving out the base_url. Remember
# though that package identifiers in PASTA are not opaque.
EntityTup = collections.namedtuple(
    "EntityTup",
    [
        "data_url",
        "base_url",
        "scope_str",
        "identifier_int",
        "version_int",
        "entity_str",
    ],
)


def download_data_entity(file_obj, data_url):
    """Download a data entity directly to disk"""
    with requests.get(data_url, stream=True) as r:
        r.raise_for_status()
        # This is a high performance way of copying a stream.
        shutil.copyfileobj(r.raw, file_obj)


def iterate_all_objects():
    scope_list = get_scope_list()
    for scope_str in scope_list:
        log.debug(f"scope {scope_str}")
        pkgid_list = get_pkgid_list(scope_str)
        for pkgid_int in pkgid_list:
            log.debug(f"  pkgid {pkgid_int}")
            revid_list = get_revid_list(scope_str, pkgid_int)
            for revid_int in revid_list:
                log.debug(f"    revid {revid_int}")


def get_scope_list():
    response = requests.get(f'{app.config["PASTA_BASE_URL"]}/eml')
    response.raise_for_status()
    return response.text.splitlines()


def get_pkgid_list(scope_str):
    response = requests.get(f'{app.config["PASTA_BASE_URL"]}/eml/{scope_str}')
    response.raise_for_status()
    return map(int, response.text.splitlines())


def get_revid_list(scope_str, pkgid_int):
    response = requests.get(f'{app.config["PASTA_BASE_URL"]}/eml/{scope_str}/{pkgid_int}')
    response.raise_for_status()
    return map(int, response.text.splitlines())


def get_solr(**query_dict):
    """Query Solr

    Example:
        get_solr(
            echoParams='all',
            defType='edismax',
            wt='json',
            q='id:*',
            fl='*',
            sort='packageid,asc',
            debug='true',
            start='0',
            rows='1',
        )

    """
    response = requests.get(f'{app.config["PASTA_BASE_URL"]}/search/eml', query_dict)
    response.raise_for_status()
    return response.text


def get_eml_url(entity_tup):
    """Get the URL to the EML that includes metadata for the given data object. E.g.,
       Data URL: https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    -> EML URL: https://pasta-d.lternet.edu/package/metadata/eml/knb-lter-ble/9/1
    """
    return '/'.join(
        [
            entity_tup.base_url,
            'metadata',
            'eml',
            get_pkg_id(entity_tup, '/'),
        ]
    )


def get_eml_path(entity_tup):
    """Get the path at which a local copy of the data object at data_url will be stored
    if it exists. E.g.,
       Data URL: https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    -> EML file path: /pasta/data/backup/data1/knb-lter-ble.9.1/Level-1-EML.xml
    """
    return pathlib.Path(
        app.config["CSV_ROOT_DIR"],
        get_pkg_id(entity_tup),
        'Level-1-EML.xml',
    ).resolve()


def get_data_url(entity_tup):
    """Get the URL to the object on PASTA given the Package ID. E.g.,
       Package ID: knb-lter-ble.9.1.0be92831cb9e173a828416a954778598
    -> Data URL: https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    """
    return '/'.join(
        [
            entity_tup.base_url,
            'data',
            'eml',
            get_pkg_id(entity_tup, "/", entity=True),
        ]
    )


def get_data_path(entity_tup):
    """Get the path at which a local copy of the data object at data_url will be stored
    if it exists. E.g.,
       Data URL: https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    -> File path: /pasta/data/backup/data1/knb-lter-ble.9.1/0be92831cb9e173a828416a954778598
    """
    return pathlib.Path(
        app.config["CSV_ROOT_DIR"],
        get_pkg_id(entity_tup, entity=True),
    ).resolve()


def get_pkg_id(entity_tup, sep_str='.', entity=False):
    t = entity_tup
    return '/'.join(
        (
            sep_str.join((str(x) for x in (t.scope_str, t.identifier_int, t.version_int))),
            *((t.entity_str,) if entity else ()),
        )
    )


def get_pkg_id_as_path(entity_tup):
    """Get the Package ID for use in a filesystem path. In this form, the Package ID has
    scope, identifier and version separated by periods, which causes those elements to
    make up a single level in the directory hierarchy. The entity name becomes the
    filename of the object.
    """
    t = entity_tup
    return f'{t.scope_str}.{t.identifier_int}.{t.version_int}/{t.entity_str}'


def get_pkg_id_as_url(entity_tup):
    """Get the Package ID for use in a PASTA URL. In this form, the Package ID has
    scope, identifier and version separated by slashes.
    """
    t = entity_tup
    return f'{t.scope_str}/{t.identifier_int}/{t.version_int}/{t.entity_str}'


def get_entity_by_data_url(data_url):
    m = DATA_URL_RX.match(data_url)
    if not m:
        raise dex.exc.DexError(f'Not a valid PASTA data URL: "{data_url}"')
    d = dict(m.groupdict())
    d["identifier_int"] = int(d.pop("id_str"))
    d["version_int"] = int(d.pop("ver_str"))
    entity_tup = EntityTup(data_url=data_url, **d)
    log.info(f'Resolved URL. data_url="{data_url}" -> entity_tup="{entity_tup}"')
    return entity_tup


def get_entity_by_local_path(data_path):
    # TODO: This is a lossy operation since the data_path doesn't have all the
    # info for creating an entity. This is since the entity is tied to a specific
    # PASTA environment.
    m = DATA_PATH_RX.match(data_path.as_posix())
    if not m:
        raise dex.exc.DexError(f'Not a valid local data path: "{data_path}"')
    n = types.SimpleNamespace(**m.groupdict())
    return EntityTup(
        data_url=(
            f'{app.config["PASTA_BASE_URL"]}/data/eml/'
            f'{n.scope_str}/{n.id_str}/{n.ver_str}/{n.entity_str}'
        ),
        base_url=app.config['PASTA_BASE_URL'],
        scope_str=n.scope_str,
        identifier_int=int(n.id_str),
        version_int=int(n.ver_str),
        entity_str=n.entity_str,
    )

def get_portal_base_by_entity(entity_tup):
    try:
        return PASTA_PORTAL_DICT[entity_tup.base_url]
    except LookupError:
        raise dex.exc.DexError(f'Not a valid PASTA BaseURL: "{entity_tup.base_url}"')

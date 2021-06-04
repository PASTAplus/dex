#!/usr/bin/env python

"""Utilities for interacting with the PASTA Restful API"""

import collections
import logging
import pathlib
import re
import shutil

import requests
from flask import current_app as app

DATA_RX_TUP = re.compile(
    r"""
    (?P<base_url>https://pasta(?:-d)?.lternet.edu/package)
    /data/eml/
    (?P<scope_str>[^/]+)/
    (?P<id_str>\d+)/
    (?P<ver_str>\d+)/
    (?P<entity_str>[a-f0-9A-F]{32,})
    $
    """,
    re.VERBOSE,
)


log = logging.getLogger(__name__)

# PASTA_BASE_URL = 'https://pasta.lternet.edu/package'
PASTA_BASE_URL = "https://pasta-d.lternet.edu/package"

# logging.getLogger('requests').setLevel(logging.INFO)
# logging.getLogger('urllib3').setLevel(logging.INFO)

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
        shutil.copyfileobj(r.raw, file_obj)


# def download_data_entity(dst_path, data_url):
#     """Download a data entity directly to disk"""
#     with requests.get(data_url, stream=True) as r:
#         with dst_path.open("wb") as f:
#             shutil.copyfileobj(r.raw, f)


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
    response = requests.get(f"{PASTA_BASE_URL}/eml")
    response.raise_for_status()
    return response.text.splitlines()


def get_pkgid_list(scope_str):
    response = requests.get(f"{PASTA_BASE_URL}/eml/{scope_str}")
    response.raise_for_status()
    return map(int, response.text.splitlines())


def get_revid_list(scope_str, pkgid_int):
    response = requests.get(f"{PASTA_BASE_URL}/eml/{scope_str}/{pkgid_int}")
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
    response = requests.get(f"{PASTA_BASE_URL}/search/eml", query_dict)
    response.raise_for_status()
    return response.text


def get_eml_url(entity_tup):
    """Get the URL to the EML that includes metadata for the given data object. E.g.,
       Data URL: https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    -> EML path: https://pasta-d.lternet.edu/package/metadata/eml/knb-lter-ble/9/1
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


# def get_pkg_id(rid):
#     t = db.get_entity(rid)
#     return f'{t.scope_str}.{t.identifier_int}.{t.version_int}'


def get_pkg_id(entity_tup, sep_str='.', entity=False):
    t = entity_tup
    return '/'.join(
        (
            sep_str.join(
                (str(x) for x in (t.scope_str, t.identifier_int, t.version_int))
            ),
            *((t.entity_str,) if entity else ()),
        )
    )


def get_pkg_id_as_url(entity_tup):
    t = entity_tup
    return f'{t.scope_str}/{t.identifier_int}/{t.version_int}/{t.entity_str}'


def get_entity_tup(data_url):
    # for rx in DATA_RX_TUP:
    m = DATA_RX_TUP.match(data_url)
    if m:
        d = dict(m.groupdict())
        d["identifier_int"] = int(d.pop("id_str"))
        d["version_int"] = int(d.pop("ver_str"))
        return EntityTup(data_url=data_url, **d)
    raise Exception(f'Not a valid Data or File URL/URI: "{data_url}"')

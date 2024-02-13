#!/usr/bin/env python

"""Utilities for interacting with the PASTA Restful API"""

import logging
import re

from flask import current_app as app

import dex.exc
import dex.util

DIST_URL_RX = re.compile(
    r"""
    (?P<base_url>.*/package)
    /data/eml/
    (?P<scope_str>[^/]+)/
    (?P<id_str>\d+)/
    (?P<ver_str>\d+)/
    (?P<entity_str>.*)
    $
    """,
    re.VERBOSE,
)

PASTA_PORTAL_DICT = {
    'https://pasta.edirepository.org/package': 'https://portal.edirepository.org/nis',
    'https://pasta-d.edirepository.org/package': 'https://portal-d.edirepository.org/nis',
    'https://pasta.lternet.edu/package': 'https://portal.edirepository.org/nis',
    'https://pasta-d.lternet.edu/package': 'https://portal-d.edirepository.org/nis',
    'https://pasta-s.lternet.edu/package': 'https://portal-s.edirepository.org/nis',
    # Dev
    'https://localhost/package': 'https://portal-d.localhost/nis',
}

log = logging.getLogger(__name__)


def get_meta_url(dist_url):
    """Derive the URL to the EML that includes metadata for the given data object. E.g.,
       dist_url: https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    -> meta_url: https://pasta-d.lternet.edu/package/metadata/eml/knb-lter-ble/9/1
    """
    d = parse_dist_url(dist_url)
    return '/'.join(
        [
            d["base_url"],
            'metadata',
            'eml',
            d["scope_str"],
            d["id_str"],
            d["ver_str"],
        ]
    )


def get_local_package_meta_path(dist_url):
    """Get the path at which a local copy of the metadata object at dist_url will be
    stored if it exists. E.g.,
       dist_url:  https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    -> meta_path: /pasta/data1/knb-lter-ble.9.1/Level-1-EML.xml
    """
    return _get_local_meta_path(app.config["LOCAL_PACKAGE_ROOT_DIR"], dist_url)


def get_local_package_data_path(dist_url):
    """Get the path at which a local copy of the data object at dist_url will be stored
    if it exists. E.g.,
       dist_url:  https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    -> data_path: /pasta/data1/knb-lter-ble.9.1/0be92831cb9e173a828416a954778598
    """
    return _get_local_data_path(app.config["LOCAL_PACKAGE_ROOT_DIR"], dist_url)


def get_local_sample_meta_path(dist_url):
    return _get_local_meta_path(app.config["LOCAL_SAMPLE_ROOT_DIR"], dist_url)


def get_local_sample_data_path(dist_url):
    return _get_local_data_path(app.config["LOCAL_SAMPLE_ROOT_DIR"], dist_url)


def is_on_pasta(meta_url):
    """Return True if the meta_url points to PASTA."""
    # TODO: This returns True for samples.
    return meta_url is None
    try:
        return get_portal_base(meta_url) is not None
    except dex.exc.DexError:
        return False


def _get_local_meta_path(base_path, dist_url):
    d = parse_dist_url(dist_url)
    return (base_path / get_pkg_id(d, '.') / 'Level-1-EML.xml').resolve()


def _get_local_data_path(base_path, dist_url):
    d = parse_dist_url(dist_url)
    return (base_path / get_pkg_id(d, '.') / d['entity_str']).resolve()


def get_pkg_id(dist_dict, sep_str):
    """Get Package ID on form scope.identifier.version. E.g.,
       dist_url: https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598
    -> pkg_id:   knb-lter-ble.9.1
    """
    return sep_str.join([dist_dict["scope_str"], dist_dict["id_str"], dist_dict["ver_str"]])


def get_portal_base(dist_url):
    d = parse_dist_url(dist_url)
    return PASTA_PORTAL_DICT.get(d["base_url"])


def parse_dist_url(dist_url):
    m = DIST_URL_RX.match(dist_url)
    if not m:
        raise dex.exc.DexError(f'Not a valid PASTA distribution URL: "{dist_url}"')
    return m.groupdict()

#!/usr/bin/env python

""""""
import collections
import logging
import re
import shutil

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://pasta.lternet.edu/package"


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


def download_data_entity(dst_path, data_url):
    """Download a data entity directly to disk"""
    with requests.get(data_url, stream=True) as r:
        with dst_path.open("wb") as f:
            shutil.copyfileobj(r.raw, f)


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
    response = requests.get(f"{BASE_URL}/eml")
    response.raise_for_status()
    return response.text.splitlines()


def get_pkgid_list(scope_str):
    response = requests.get(f"{BASE_URL}/eml/{scope_str}")
    response.raise_for_status()
    return map(int, response.text.splitlines())


def get_revid_list(scope_str, pkgid_int):
    response = requests.get(f"{BASE_URL}/eml/{scope_str}/{pkgid_int}")
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
    response = requests.get(f"{BASE_URL}/search/eml", query_dict)
    response.raise_for_status()
    return response.text


def pasta_data_to_eml_url(data_url):
    """Get the URL to the EML that includes metadata for the given data object

    Example:
        Data URL:
        https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/0be92831cb9e173a828416a954778598

        -> EML path:
        https://pasta-d.lternet.edu/package/metadata/eml/knb-lter-ble/9/1
    """
    e = parse_pasta_data_url(data_url)
    return (
        f"{e.base_url}/metadata/eml/{e.scope_str}/"
        f"{e.identifier_int}/{e.version_int}"
    )


def parse_pasta_data_url(data_url):
    m = re.match(
        r"(?P<base_url>https://pasta(?:-d)?.lternet.edu/package)/data/eml/"
        r"(?P<scope_str>[^/]+)/(?P<id_str>\d+)/(?P<ver_str>\d+)/"
        r"(?P<entity_str>[a-f0-9A-F]{32,})$",
        data_url,
    )
    if not m:
        raise Exception(f'Invalid PASTA Data URL: "{data_url}"')
    d = dict(m.groupdict())
    d["identifier_int"] = int(d.pop("id_str"))
    d["version_int"] = int(d.pop("ver_str"))
    return EntityTup(data_url=data_url, **d)

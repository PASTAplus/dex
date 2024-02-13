import logging
import pathlib

import lxml.etree
from flask import current_app as app

import dex.cache
import dex.eml_extract
import dex.pasta

log = logging.getLogger(__name__)


# @dex.cache.disk("sample_data_entity_list", "list")
def get_sample_data_entity_list(_rid):
    data_entity_list = []
    sample_root_path = app.config['LOCAL_SAMPLE_ROOT_DIR']
    log.debug(f'Looking for local sample packages at: {sample_root_path.as_posix()}')
    for sample_eml_path in sample_root_path.glob('**/*'):
        if sample_eml_path.is_file() and sample_eml_path.suffix == '.xml':
            for dist_url in _get_sample_dist_url_list(sample_eml_path):
                sample_data_path = dex.pasta.get_local_sample_data_path(dist_url)
                if sample_data_path.exists():
                    dist_url_dict = dex.pasta.parse_dist_url(dist_url)
                    data_entity_list.append(
                        dict(
                            **dist_url_dict,
                            **dict(
                                dist_url=dist_url,
                                size=sample_data_path.stat().st_size,
                                abs_path=sample_eml_path,
                            ),
                        )
                    )

    return sorted(data_entity_list, key=lambda d: d['size'])


def _split_path(sample_path: pathlib.Path):
    """Given a local path to a data object, return the dist_url for that object.

    sample_path: /home/pasta/dev/dex/dex-samples/knb-lter-pie.41.4/e7c0f7794b67386747cf6a5920853093
    -> https://sample/package/data/eml/knb-lter-pie/41/4/e7c0f7794b67386747cf6a5920853093
    """
    pkg_id, entity_str = sample_path.parts[-2:]
    scope_str, id_str, ver_str = pkg_id.split('.')
    return dict(
        dist_url=f'https://sample/package/data/eml/{scope_str}/{id_str}/{ver_str}/{entity_str}',
        scope_str=scope_str,
        id_str=id_str,
        ver_str=ver_str,
        entity_str=entity_str,
    )


def _get_sample_dist_url_list(eml_path):
    """Given a local path to an EML doc, return a list of dist_urls for the data
    objects.
    """
    dist_url_list = []
    eml_el = lxml.etree.parse(eml_path.as_posix())
    for dist_url in dex.eml_extract.get_dist_url_list(eml_el):
        dist_url_list.append(dist_url)
    return dist_url_list

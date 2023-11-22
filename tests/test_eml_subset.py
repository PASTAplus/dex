import logging
import pathlib
import pprint
import random
import subprocess
import sys

import _pytest.pathlib
import lxml
import numpy as np
import pandas as pd
import ydata_profiling
import lxml.etree

import dex.eml_subset
import dex.main
import dex.util

log = logging.getLogger(__name__)

dex_path = _pytest.pathlib.Path(__file__).resolve().parents[2]
yml_config_path = (dex_path / 'profiling_config.yml').as_posix()

"""
 80K tests/test_docs/knb-lter-mcr.19.13/Level-1-EML.xml
 82K tests/test_docs/knb-lter-hfr.85.16/Level-1-EML.xml
 84K tests/test_docs/edi.621.1/Level-1-EML.xml
 89K tests/test_docs/knb-lter-gce.292.20/Level-1-EML.xml
 89K tests/test_docs/knb-lter-sgs.138.17/Level-1-EML.xml
 94K tests/test_docs/knb-lter-hfr.174.10/Level-1-EML.xml
107K tests/test_docs/knb-lter-gce.326.22/Level-1-EML.xml
108K tests/test_docs/knb-lter-cap.277.8/Level-1-EML.xml
117K tests/test_docs/knb-lter-cwt.3033.26/Level-1-EML.xml
119K tests/test_docs/knb-lter-jrn.210437100.6/Level-1-EML.xml
124K tests/test_docs/knb-lter-hfr.35.16/Level-1-EML.xml
139K tests/test_docs/knb-lter-jrn.210437082.17/Level-1-EML.xml
177K tests/test_docs/knb-lter-jrn.210548066.3/Level-1-EML.xml
178K tests/test_docs/knb-lter-jrn.210548079.1/Level-1-EML.xml
332K tests/test_docs/knb-lter-mcr.32.30/Level-1-EML.xml
589K tests/test_docs/edi.115.1/Level-1-EML.xml
1.1M tests/test_docs/knb-lter-luq.75.11034157/Level-1-EML.xml
"""

EML_DOC_1 = pathlib.Path(
    '/home/dahl/dev/dex/tests/test_docs/knb-lter-luq.75.11034157/Level-1-EML.xml'
)

# fmt:off
EML_DOC_2 = pathlib.Path(
    '/home/dahl/dev/dex-data/___samples/knb-lter-jrn.210437031.6/Level-1-EML.xml'
)
EML_FILTER_DICT_2 = {
    "category_filter": [[6, ["300"]]],
    "column_filter": [
        True, True, True, True, True, True, True, True, True, True, True, True, True,
        True, True, True, True, True, True, True, True, True, True, True, True, True,
        True, True, True, True, True, True, True, True, True, True, True, True, True,
        True,
    ],
    "date_filter": {"col_name": "Date", "end": "2019-11-04", "start": "2013-11-05"},
    "query_filter": "",
    "row_filter": {"a": 10, "b": 100},
}
# fmt:on

# fmt:off
EML_DOC_3 = pathlib.Path('tests/test_docs/knb-lter-luq.75.11034157/Level-1-EML.xml')
EML_FILTER_DICT_3 = {
    # "category_filter": [
    #     [
    #         6,
    #         [
    #             "300"
    #         ]
    #     ]
    # ],
    # "column_filter": [
    #     True, True, True, True, True, True, True, True, True, True, True, True, True,
    #     True, True, True, True, True, True, True, True, True, True, True, True, True,
    #     True, True, True, True, True, True, True, True, True, True, True, True, True,
    #     True
    # ],
    # "date_filter": {
    #     "col_name": "Date",
    #     "end": "2019-11-04",
    #     "start": "2013-11-05"
    # },
    # "query_filter": "",
    "row_filter": {"a": 10, "b": 100},
}
# fmt:on

# fmt:off
EX_4_PATH = 'knb-lter-luq.75.11034157/Level-1-EML.xml'
EX_4_PKG_PATH = 'https://pasta.lternet.edu/package/data/eml/knb-lter-luq/75/11034157/e59392e57cae2a9f253729bf9e6b5d95'
# fmt:on


# log.debug("=" * 100)
# log.debug('create_eml()')
# log.debug(pprint.pformat({"rid": rid, "filter_dict": filter_dict}))
# log.debug("=" * 100)


def get_eml_etree_by_path(eml_path: pathlib.Path):
    return lxml.etree.parse(eml_path.as_posix())


def test_0100(docs_path):
    eml_path = docs_path / EX_4_PATH
    eml_el = get_eml_etree_by_path(eml_path)
    url_el = eml_el.xpath(".//url[@function='download']/text()")
    log.debug(f'eml_el="{dex.util.get_etree_as_pretty_printed_xml(eml_el)}"')


def test_1000(docs_path):
    """_prune_data_table()"""
    eml_path = docs_path / EX_4_PATH
    eml_el = get_eml_etree_by_path(eml_path)
    dex.eml_subset._prune_data_table(eml_el, EX_4_PKG_PATH)
    log.debug(f'eml_el="{dex.util.get_etree_as_pretty_printed_xml(eml_el)}"')


def test_1010(client, docs_path):
    """_subset_eml()"""
    eml_path = docs_path / EX_4_PATH
    eml_el = get_eml_etree_by_path(eml_path)
    dex.eml_subset._subset_eml(
        eml_el,
        EX_4_PKG_PATH,
        row_count=1234,
        byte_count=5678,
        md5_checksum='baadf00d',
        col_list=tuple(i&1for i in range(100)),
    )
    log.debug(f'eml_el="{dex.util.get_etree_as_pretty_printed_xml(eml_el)}"')

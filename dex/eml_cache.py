"""PASTA and EML utils"""

import datetime
import logging

import lxml.etree

import dex.db
import dex.cache
import dex.csv_parser
import dex.obj_bytes
import dex.eml_types
import dex.eml_types
import dex.exc
import dex.pasta
import dex.util

"""
* default:
* emacs:
* friendly:
* colorful:
* autumn:
* murphy:
* manni:
* monokai:
* perldoc:
* pastie:
* borland:
* trac:
* native:
* fruity:
* bw:
* vim:
* vs:
* tango:
* rrt:
* xcode:
* igor:
* paraiso-light:
* paraiso-dark:
* lovelace:
* algol:
* algol_nu:
* arduino:
* rainbow_dash:
* abap:
* solarized-dark:
* solarized-light:
* sas:
* stata:
* stata-light:
* stata-dark:
* inkpot:
"""

# fmt:off
PYGMENTS_STYLE_LIST = [
    'default', 'emacs', 'friendly', 'colorful', 'autumn', 'murphy', 'manni', 'monokai',
    'perldoc', 'pastie', 'borland', 'trac', 'native', 'fruity', 'bw', 'vim', 'vs',
    'tango', 'rrt', 'xcode', 'igor', 'paraiso-light', 'paraiso-dark', 'lovelace',
    'algol', 'algol_nu', 'arduino', 'rainbow_dash', 'abap', 'solarized-dark',
    'solarized-light', 'sas', 'stata', 'stata-light', 'stata-dark', 'inkpot',
]
# fmt:on

log = logging.getLogger(__name__)

# Default start and end datetimes used in the UI if the EML lists one or more datetime
# columns, but no datetime ranges.
FALLBACK_BEGIN_DATETIME = datetime.datetime(
    2000,
    1,
    1,
)
FALLBACK_END_DATETIME = datetime.datetime(
    2040,
    1,
    1,
)

DATE_FORMAT_DICT = {
    'YYYY': '%Y',
    'YYYY-MM-DD': '%Y-%m-%d',
    'YYYY-MM-DD HH:MM:SS': '%Y-%m-%d %H:%M:%S',
    'MM': '%M',
    'MM/DD/YYYY': '%M/%D/%Y',
    'HHMM': '%H%M',
}


# @dex.cache.disk('eml-dt-cols', 'list')
def get_datetime_columns(rid):
    type_list = dex.csv_parser.get_derived_dtypes_from_eml(rid)
    dt_col_list = []
    for col_idx, dtype_dict in enumerate(type_list):
        if dtype_dict['type_str'] == 'TYPE_DATE':
            dt_col_list.append(dtype_dict)
    return dt_col_list


def get_categorical_columns(rid):
    type_list = dex.csv_parser.get_derived_dtypes_from_eml(rid)
    dt_col_list = []
    for col_idx, dtype_dict in enumerate(type_list):
        if dtype_dict['type_str'] == 'TYPE_CAT':
            dt_col_list.append(dtype_dict)
    return dt_col_list


def get_datetime_columns_as_dict(rid):
    return {d['col_idx']: d for d in get_datetime_columns(rid)}


# etree formatting


def get_attributes_as_highlighted_html(rid):
    """Get column descriptions for a given CSV file. The columns are described in EML
    attribute elements.
    """
    return {
        k: dex.util.get_etree_as_highlighted_html(v)
        for k, v in get_attributes_as_etree(rid).items()
    }


@dex.cache.disk('eml-highlighted', 'pickle')
def get_eml_as_highlighted_html(rid):
    root_el = get_eml_etree(rid)
    try:
        root_el = root_el.getroottree()
    except AttributeError:
        pass
    return dex.util.get_etree_as_highlighted_html(root_el)


# Read from the EML doc

# Read the EML attribute fragments that declare the types and other information for each
# column in the CSV documents.

# noinspection PyUnresolvedReferences
# @dex.cache.disk('attributes-tree', 'etree')
# TODO: Find best way to cache dict of etree.
def get_attributes_as_etree(rid):
    dt_el = get_data_table(rid)
    return {
        # Using this pattern, of creating a dict with the column index as the key
        # since we can't be sure that the attributes will always be stored in column
        # order in the EML doc.
        col_idx: attr_el
        for col_idx, attr_el in enumerate(dt_el.xpath('.//attributeList/attribute'))
    }


def get_csv_name(dt_el):
    return dex.eml_types.first_str(dt_el, './/physical/objectName/text()')


@dex.cache.disk('datatable', 'etree')
def get_data_table(rid):
    entity_tup = dex.db.get_entity(rid)
    pkg_path = dex.pasta.get_pkg_id_as_url(entity_tup)
    eml_el = get_eml_etree(rid)
    dt_el = dex.eml_types.get_data_table_by_package_id(eml_el, pkg_path)
    return dt_el


# Full EML


@dex.cache.disk('eml', 'xml')
def get_eml_xml(rid):
    root_el = get_eml_etree(rid)
    return dex.util.get_etree_as_pretty_printed_xml(root_el)


@dex.cache.disk('eml', 'etree')
def get_eml_etree(rid):
    # if isinstance(rid, pathlib.Path):
    #     eml_path = rid
    # else:
    eml_path = dex.obj_bytes.open_eml(rid)
    return lxml.etree.parse(eml_path.as_posix())

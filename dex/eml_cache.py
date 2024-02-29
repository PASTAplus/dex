"""PASTA and EML utils"""

import datetime
import logging

import lxml.etree

import dex.cache
import dex.csv_parser
import dex.db
import dex.eml_extract
import dex.exc
import dex.obj_bytes
import dex.pasta
import dex.util

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
        if dtype_dict['pandas_type'] == dex.eml_extract.PandasType.DATETIME:
            dt_col_list.append(dtype_dict)
    return dt_col_list


def get_categorical_columns(rid):
    type_list = dex.csv_parser.get_derived_dtypes_from_eml(rid)
    dt_col_list = []
    for col_idx, dtype_dict in enumerate(type_list):
        if dtype_dict['pandas_type'] == dex.eml_extract.PandasType.CATEGORY:
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
    dt_el = get_data_table_el(rid)
    return {
        # Using this pattern, of creating a dict with the column index as the key
        # since we can't be sure that the attributes will always be stored in column
        # order in the EML doc.
        col_idx: attr_el
        for col_idx, attr_el in enumerate(dt_el.xpath('.//attributeList/attribute'))
    }


@dex.cache.disk('csv_name', 'text')
def get_csv_name(rid):
    dt_el = get_data_table_el(rid)
    return dex.eml_extract.first_str_orig(dt_el, './/physical/objectName/text()')


# @dex.cache.disk('pkg_id_str', 'text')
def get_pkg_id_str(rid):
    """Return the Package ID for the given rid.

    E.g., 'knb-lter-pie.41.4'
    """
    eml_el = get_eml_etree(rid)
    return dex.eml_extract.first_str_orig(eml_el, '/eml:eml/@packageId')


def get_pkg_id_dict(rid):
    """Return the Package ID for the given rid as a dict.

    E.g., {
        scope_str: 'knb-lter-pie
        id_str: 41
        ver_str: 4
    }

    Return None if the Package ID is missing or not in the expected format.
    """
    id_str = get_pkg_id_str(rid)
    try:
        scope_str, id_str, ver_str = id_str.split('.')
    except ValueError:
        return None
    return {
        'scope_str': scope_str,
        'id_str': id_str,
        'ver_str': ver_str,
    }


# @dex.cache.disk('datatable', 'etree')
def get_data_table_el(rid):
    dist_url = dex.db.get_dist_url(rid)
    eml_el = get_eml_etree(rid)
    dt_el = dex.eml_extract.get_data_table_by_dist_url(eml_el, dist_url)
    # log.debug(f'dt_el="{dex.util.get_etree_as_pretty_printed_xml(dt_el)}"')
    return dt_el


@dex.cache.disk('eml', 'xml')
def get_eml_xml(rid):
    root_el = get_eml_etree(rid)
    return dex.util.get_etree_as_pretty_printed_xml(root_el)


@dex.cache.disk('eml', 'etree')
def get_eml_etree(rid):
    eml_path = dex.obj_bytes.open_eml(rid)
    try:
        return lxml.etree.parse(eml_path.as_posix())
    except lxml.etree.LxmlError as e:
        raise dex.exc.EMLError(
            f'Unable to parse EML. error="{str(e)}". path="{eml_path.as_posix()}"'
        )

"""EML type utils"""

import datetime
import io
import logging
import pprint

import lxml.etree

# import dex.exc

log = logging.getLogger(__name__)

# Default start and end datetimes used in the UI if the EML lists one or more datetime
# columns, but no datetime ranges.
FALLBACK_START_DATETIME = datetime.datetime(2000, 1, 1)
FALLBACK_END_DATETIME = datetime.datetime(2040, 1, 1)


DATE_FORMAT_DICT = {
    'YYYY': '%Y',
    'YYYY-MM-DD': '%Y-%m-%d',
    'YYYY-MM-DD HH:MM:SS': '%Y-%m-%d %H:%M:%S',
    'YYYY-MM-DDTHH:MM:SS': '%Y-%m-%dT%H:%M:%S',
    'MM': '%M',
    'MM/DD/YYYY': '%M/%D/%Y',
    'HHMM': '%H%M',
}


def get_profiling_types(dt_el):
    """
    TYPE_DATE           # A date variable
    TYPE_NUM            # A numeric variable
    TYPE_CAT            # A categorical variable
    S_TYPE_UNSUPPORTED  # An unsupported variable
    """
    type_list = []

    # Iterate over 'attribute', which describes each individual column.
    attr_list = list(dt_el.xpath('.//attributeList/attribute'))

    for i, attr_el in enumerate(attr_list):
        # log.info()

        attribute_name = first_str(attr_el, './/attributeName/text()')

        storage_type = first_str(attr_el, './/storageType/text()')
        iso_date_format_str = first_str(
            attr_el, './/measurementScale/dateTime/formatString/text()'
        )
        number_type = first_str(
            attr_el, './/measurementScale/ratio/numericDomain/numberType/text()'
        )
        # numeric_domain = first_str(
        #     attr_el, './/measurementScale/ratio/unit/numericDomain/text()'
        # )
        is_enumarated = has(attr_el, './/nonNumericDomain/enumeratedDomain')
        ratio = first_str(attr_el, './/measurementScale/ratio/text()')

        # log.debug(f'Raw extracted:')
        # log.debug(f'  attribute_name={attribute_name}')
        # log.debug(f'  storage_type={storage_type}')
        # log.debug(f'  iso_date_format_str={iso_date_format_str}')
        # log.debug(f'  number_type={number_type}')
        # log.debug(f'  is_enumarated={is_enumarated}')
        # log.debug(f'  ratio={ratio}')

        type_dict = {
            'i': i,
            'attribute_name': attribute_name,
            'type_str': 'S_TYPE_UNSUPPORTED',
            'storage_type': storage_type,
            'iso_date_format_str': iso_date_format_str,
            'c_date_format_str': None,
            'number_type': number_type,
            'is_enumerated': is_enumarated,
            'ratio': ratio,
        }

        # Supported date formats

        if attribute_name == 'YEAR' and iso_date_format_str is None:
            iso_date_format_str = 'YYYY'

        if iso_date_format_str in DATE_FORMAT_DICT.keys():
            type_dict['type_str'] = 'TYPE_DATE'
            type_dict['iso_date_format_str'] = iso_date_format_str
            type_dict['c_date_format_str'] = DATE_FORMAT_DICT[iso_date_format_str]

        # Supported number formats

        elif number_type and number_type.lower() in (
            'real',
            'integer',
            'whole',
            'natural',
        ):
            type_dict['type_str'] = 'TYPE_NUM'

        elif storage_type and storage_type.lower() in (
            'float',
            'floating-point',
            'integer',
        ):
            type_dict['type_str'] = 'TYPE_NUM'

        # Categorical data

        elif is_enumarated:
            type_dict['type_str'] = 'TYPE_CAT'

        type_list.append(type_dict)

        # This shows the attibute EML fragment and the resulting derived type info.
        # plog({'idx': i, 'name': attribute_name}, '%' * 100, log.info)
        # plog(attr_el, 'attr_el', log.info)
        # plog(type_dict, 'type_dict', log.info)

    return type_list


def has(el, xpath):
    return first(el, xpath) is not None


def first(el, xpath):
    """Return the first match to the xpath if there was a match, else None. Can this be
    done directly in xpath 1.0?
    """
    # log.debug(f'first() xpath={xpath} ...')
    res_el = el.xpath(f'({xpath})[1]')
    try:
        el = res_el[0]
    except IndexError:
        el = None
    log.debug(f'first() -> {el}')
    return el


def first_str(el, text_xpath):
    """Apply xpath and, if there is a match, assume that the match is a text node, and
    convert it to an uppper case string. {text_xpath} is an xpath that returns a text
    node. E.g., `.//text()`.
    """
    el = first(el, text_xpath)
    return str(el).upper().strip() if el else None


def get_data_table_list(root_el):
    """Return list of dataTable elements in EML doc"""
    if not root_el:
        return []
    return root_el.xpath('.//dataset/dataTable')


def plog(obj, msg=None, logger=log.debug):
    if lxml.etree.iselement(obj):
        obj_str = pretty_format_fragment(obj)
    else:
        obj_str = pprint.pformat(obj)
    logger("-" * 100)
    if msg:
        logger(f"{msg}:")
    tuple(
        map(logger, tuple(f"  {line}" for line in obj_str.splitlines())),
    )
    logger("")


def pretty_format_fragment(el):
    if not isinstance(el, list):
        el = [el]
    buf = io.BytesIO()
    for e in el:
        buf.write(lxml.etree.tostring(e, pretty_print=True))
    return buf.getvalue().decode('utf-8')

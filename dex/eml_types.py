"""EML type utils

This module works directly with EML XML objects in the lxml.etree domain, and so can
be used without having an `rid`.
"""
import contextlib
import datetime
import logging
import re

import dateutil.parser

import dex.exc

# This module should not require cache access and so, should not import `dex.eml_cache`
# or `dex.cache`.

log = logging.getLogger(__name__)

# Default start and end datetimes used in the UI if the EML lists one or more datetime
# columns, but no datetime ranges.
FALLBACK_BEGIN_DATETIME = datetime.datetime(2000, 1, 1)
FALLBACK_END_DATETIME = datetime.datetime(2040, 1, 1)

# Supported date-format strings. Ordered by how much information they extract.
# List of formatting directives:
# https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
DATE_FORMAT_DICT = {
    'YYYY-MM-DD HH:MM:SS': '%Y-%m-%d %H:%M:%S',
    'YYYY-MM-DDTHH:MM:SS': '%Y-%m-%dT%H:%M:%S',
    'YYYY-MM-DD': '%Y-%m-%d',
    'MM/DD/YYYY': '%m/%d/%Y',
    'DD-MON-YYYY': '%d-%b-%Y',
}

DATE_INT_FORMAT_DICT = {
    'YYYY': '%Y',
    'HHMM': '%H%M',
    'MM': '%m',
    'HH:MM:SS': '%H:%M:%S',
}


def get_derived_dtypes_from_eml(dt_el):
    """Heuristics to find a Pandas / NumPy type, called dtype, to use when processing
    a CSV file that has EML based type declarations.

    Pandas supports a basic set of types, while EML supports much more complex type
    declarations and descriptions. The columns for which we are able to derive a dtype
    are supported with additional functionality in other areas of of the app.

    Pandas         dtype         Python type       NumPy type       Usage
    object         str or mixed  string_, unicode_, mixed types    Text or mixed numeric and non-numeric values
    int64          int           int_, int8, int16, int32, int64, uint8, uint16, uint32, uint64    Integer numbers
    float64        float         float_, float16, float32, float64    Floating point numbers
    bool           bool          bool_    True/False values
    datetime64     datetime      datetime64[ns]    Date and time values
    timedelta[ns]  NA            NA    Differences between two datetimes
    category       NA            NA    Finite list of text values

    TYPE_DATE           - A date variable
    TYPE_NUM            - A numeric variable
    TYPE_CAT            - A categorical variable
    S_TYPE_UNSUPPORTED  - An unsupported variable

    dtype_dict = {
        'col_idx': col_idx,
        'col_name': col_name,
        'type_str': 'S_TYPE_UNSUPPORTED',
        'storage_type': storage_type,
        'date_fmt_str': date_fmt_str,
        'c_date_fmt_str': None,
        'number_type': number_type,
        'numeric_domain': numeric_domain,
        'ratio': ratio,
        'missing_value_list': missing_value_list,
        'col_agg_dict': col_agg_dict,
    }
    """
    default_dt = get_default_begin_end_datetime_range(dt_el)

    type_list = []

    # Iterate over 'attribute', which describes each individual column.
    attr_list = list(dt_el.xpath('.//attributeList/attribute'))

    for col_idx, attr_el in enumerate(attr_list):
        # log.debug()

        col_name = first_str_orig(attr_el, './/attributeName/text()')

        storage_type = first_str(attr_el, './/storageType/text()')
        date_fmt_str = first_str(
            attr_el, './/measurementScale/dateTime/formatString/text()'
        )
        number_type = first_str(
            attr_el, './/measurementScale/ratio/numericDomain/numberType/text()'
        )
        # numeric_domain = first_str(
        #     attr_el, './/measurementScale/ratio/unit/numericDomain/text()'
        # )
        is_enumerated = has(attr_el, './/nonNumericDomain/enumeratedDomain')
        ratio = first_str(attr_el, './/measurementScale/ratio/text()')
        missing_code_list = multiple_str(attr_el, './/missingValueCode/code/text()')

        # log.debug(f'Raw extracted:')
        # log.debug(f'  col_name={col_name}')
        # log.debug(f'  storage_type={storage_type}')
        # log.debug(f'  date_fmt_str={date_fmt_str}')
        # log.debug(f'  number_type={number_type}')
        # log.debug(f'  is_enumerated ={is_enumerated }')
        # log.debug(f'  ratio={ratio}')
        # log.debug(f'  missing_value_list={missing_value_list}')
        # log.debug(f'  col_agg_dict={col_agg_dict}')

        dtype_dict = {
            'col_idx': col_idx,
            'col_name': col_name,
            'type_str': 'S_TYPE_UNSUPPORTED',
            'storage_type': storage_type,
            'date_fmt_str': date_fmt_str,
            'c_date_fmt_str': None,
            'number_type': number_type,
            'is_enumerated': is_enumerated ,
            'ratio': ratio,
            'missing_code_list': missing_code_list,
        }

        # Supported date formats

        if col_name.upper() == 'YEAR' and date_fmt_str is None:
            date_fmt_str = 'YYYY'

        if date_fmt_str in DATE_INT_FORMAT_DICT.keys():
            dtype_dict['type_str'] = 'TYPE_INT'
            dtype_dict['date_fmt_str'] = date_fmt_str
            date_fmt_str = DATE_INT_FORMAT_DICT[date_fmt_str]
            dtype_dict['c_date_fmt_str'] = date_fmt_str
            begin_date_str = first_str(attr_el, './/beginDate/calendarDate/text()')
            end_date_str = first_str(attr_el, './/endDate/calendarDate/text()')
            dtype_dict['begin_dt'] = (
                try_date_time_parse(begin_date_str)
                if begin_date_str
                else default_dt.begin_dt
            )
            dtype_dict['end_dt'] = (
                try_date_time_parse(end_date_str) if end_date_str else default_dt.end_dt
            )

        elif date_fmt_str in DATE_FORMAT_DICT.keys():
            dtype_dict['type_str'] = 'TYPE_DATE'
            dtype_dict['date_fmt_str'] = date_fmt_str
            date_fmt_str = DATE_FORMAT_DICT[date_fmt_str]
            dtype_dict['c_date_fmt_str'] = date_fmt_str
            begin_date_str = first_str(attr_el, './/beginDate/calendarDate/text()')
            end_date_str = first_str(attr_el, './/endDate/calendarDate/text()')
            dtype_dict['begin_dt'] = (
                try_date_time_parse(begin_date_str)
                if begin_date_str
                else default_dt.begin_dt
            )
            dtype_dict['end_dt'] = (
                try_date_time_parse(end_date_str) if end_date_str else default_dt.end_dt
            )

        # Supported number formats

        elif number_type and number_type.lower() in (
            'real',
            'integer',
            'whole',
            'natural',
        ):
            dtype_dict['type_str'] = 'TYPE_NUM'

        elif storage_type and storage_type.lower() in (
            'float',
            'floating-point',
            'integer',
        ):
            dtype_dict['type_str'] = 'TYPE_NUM'
            dtype_dict['number_type'] = storage_type.lower()

        # Categorical data

        elif is_enumerated :
            dtype_dict['type_str'] = 'TYPE_CAT'

        type_list.append(dtype_dict)

        # This shows the attribute EML fragment and the resulting derived_dtype type info.
        # logpp({'idx': col_idx, 'name': col_name}, '%' * 100, log.debug)
        # logpp(attr_el, 'attr_el', log.debug)
        # logpp(dtype_dict, 'dtype_dict', log.debug)

    return type_list


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
    # log.debug(f'first() -> {el}')
    return el


def first_str(el, text_xpath):
    """Apply xpath and, if there is a match, assume that the match is a text node, and
    convert it to an upper case string. {text_xpath} is an xpath that returns a text
    node. E.g., `.//text()`.
    """
    el = first(el, text_xpath)
    return str(el).upper().strip() if el else None


def first_str_orig(el, text_xpath):
    el = first(el, text_xpath)
    return str(el) if el else None


def multiple_str(el, text_xpath):
    el = el.xpath(text_xpath)
    return [str(x) for x in el] if el else None


def has(el, xpath):
    return first(el, xpath) is not None


# TODO: dt_fmt
def first_date(el, date_xpath, dt_fmt=None):
    """Apply xpath and, if there is a match, try to parse the result as a date-time.

    Args:
        el:
        date_xpath: xpath that returns a date-time string in a text node. E.g., `.//text()`.
        dt_fmt: If is provided, tried as the first date-time parse format.

    If dt_fmt is not providing, or if parsing with dt_fmt fails, parsing is tried
    with ISO formats, then with other common date-time formats.
    """
    el = first(el, date_xpath)
    return str(el).upper().strip() if el else None


def get_data_table_list(root_el):
    """Return list of dataTable elements in EML doc"""
    if not root_el:
        return []
    return root_el.xpath('.//dataset/dataTable')


def get_default_begin_end_datetime_range(el):
    begin_str = first_str(
        el,
        './/dataset/coverage/temporalCoverage/rangeOfDates/beginDate/calendarDate/text()',
    )
    end_str = first_str(
        el,
        './/dataset/coverage/temporalCoverage/rangeOfDates/endDate/calendarDate/text()',
    )
    return N(
        begin_dt=try_date_time_parse(begin_str)
        if begin_str
        else FALLBACK_BEGIN_DATETIME,
        end_dt=try_date_time_parse(end_str) if end_str else FALLBACK_END_DATETIME,
    )


# def get_iso_date_time(iso_str):
#     """Try to parse as ISO8601. Return a string on form 'YYYY-MM-DD' if parsing is
#     successful, else None.
#     """
#     with contextlib.suppress(ValueError, TypeError):
#         return dateutil.parser.isoparse(iso_str).strftime('%Y-%m-%d')


def try_date_time_parse(dt_str, dt_fmt=None):
    """Apply xpath and, if there is a match, try to parse the result as a date-time.

    Args:
        dt_str (str): A date-time, date, or time.
        dt_fmt: If is provided, tried as the first date-time parse format.

    If dt_fmt is not provided, or if parsing with dt_fmt fails, parsing is tried
    with ISO formats, then with other common date-time formats.
    """
    if dt_fmt:
        with contextlib.suppress(ValueError, TypeError):
            return datetime.datetime.strptime(dt_str, dt_fmt)
    with contextlib.suppress(ValueError, TypeError):
        return dateutil.parser.isoparse(dt_str)
    for dt_fmt in DATE_FORMAT_DICT.values():
        return datetime.datetime.strptime(dt_str, dt_fmt)


def get_data_table_by_data_url(el, data_url):
    for dt_el in el.xpath('.//dataset/dataTable'):
        url = first_str(dt_el, './/physical/distribution/online/url/text()')
        url = url[url.find('/PACKAGE/') :]
        if url == data_url.as_posix().upper():
            return dt_el
    raise dex.exc.EMLError(f'Missing DataTable in EML. data_url="{data_url}"')


def get_data_table_by_package_id(el, pkg_path):
    for dt_el in el.xpath('.//dataset/dataTable'):
        url = first_str(dt_el, './/physical/distribution/online/url/text()')
        # log.debug(f'{url}')
        # log.debug(f'{pkg_path}')
        if url and url.lower().endswith(pkg_path):
            return dt_el
    raise dex.exc.EMLError(f'Missing DataTable in EML. pkg_path="{pkg_path}"')

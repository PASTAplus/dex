"""EML type utils

This module works directly with EML XML objects in the lxml.etree domain, and so can
be used without having an `rid`.
"""
import contextlib
import csv
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
FALLBACK_END_DATETIME = datetime.datetime.now()

DEFAULT_HEADER_LINE_COUNT = 0
DEFAULT_FOOTER_LINE_COUNT = 0
DEFAULT_RECORD_DELIMITER = r'\n'
DEFAULT_FIELD_DELIMITER = ','
DEFAULT_QUOTE_CHARACTER = '"'

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

ISO8601_TO_CTIME_DICT = {
    # %a
    # Weekday as locale’s abbreviated name.
    # Sun, Mon, …, Sat (en_US);
    'DDD': '%a',
    'w': '%a',
    # %A
    # Weekday as locale’s full name.
    # Sunday, Monday, …, Saturday (en_US);
    'W': '%a',
    # %w
    # Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
    # 0, 1, …, 6
    'WW': '%w',
    # %d
    # Day of the month as a zero-padded decimal number.
    # 01, 02, …, 31
    'DD': '%d',
    'dd': '%d',
    # %b
    # Month as locale’s abbreviated name.
    # Jan, Feb, …, Dec (en_US);
    'MON': '%b',
    'mon': '%b',
    # %B
    # Month as locale’s full name.
    # January, February, …, December (en_US);
    # %y
    # Year without century as a zero-padded decimal number.
    # 00, 01, …, 99
    'YY': '%y',
    'yy': '%y',
    # %Y
    # Year with century as a decimal number.
    # 0001, 0002, …, 2013, 2014, …, 9998, 9999
    'YYYY': '%Y',
    'yyyy': '%Y',
    # %H
    # Hour (24-hour clock) as a zero-padded decimal number.
    # 00, 01, …, 23
    'HH': '%H',
    'HH24': '%H',
    'hh': '%H',
    'h': '%H',
    # %I
    # Hour (12-hour clock) as a zero-padded decimal number.
    # 01, 02, …, 12
    # %p
    # Locale’s equivalent of either AM or PM.
    # AM, PM (en_US);
    # %M
    # Minute as a zero-padded decimal number.
    # 00, 01, …, 59
    'MM': '%m',
    'mm': '%M',
    'm': '%M',
    # %S
    # Second as a zero-padded decimal number.
    # 00, 01, …, 59
    'SS': '%S',
    'ss': '%S',
    's': '%M',
    # %f
    # Microsecond as a decimal number, zero-padded on the left.
    # 000000, 000001, …, 999999
    #
    # %z
    # UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive).
    # (empty), +0000, -0400, +1030, +063415, -030712.345216
    #
    # %Z
    # Time zone name (empty string if the object is naive).
    # (empty), UTC, GMT
    #
    # %j
    # Day of the year as a zero-padded decimal number.
    # 001, 002, …, 366
    'DDDD': '%j',
    # %U
    # Week number of the year (Sunday as the first day of the week) as a zero padded decimal number. All days in a new year preceding the first Sunday are considered to be in week 0.
    # 00, 01, …, 53
    #
    # %W
    # Week number of the year (Monday as the first day of the week) as a decimal number. All days in a new year preceding the first Monday are considered to be in week 0.
    # 00, 01, …, 53
    #
    # %c
    # Locale’s appropriate date and time representation.
    # Tue Aug 16 21:30:00 1988 (en_US);
    #
    # %x
    # Locale’s appropriate date representation.
    # 08/16/1988 (en_US);
    #
    # Locale’s appropriate time representation.
    # 21:30:00 (en_US);
    # %m
    # Month as a zero-padded decimal number.
    # 01, 02, …, 12
}


def iso8601_to_c_format(iso_str):
    """Convert an ISO8601-style datetime format spec, as used in EML, to a C-style
    format spec, as used by the Python datetime formatting functions.
    """
    c_list = []
    for x in re.split(
        '(\W|integer|alphabetic|[T]|MST|YYYY|YY|MM|DDD|DD|HH|HH24|SS|mon)',
        iso_str,
        flags=re.IGNORECASE,
    ):
        if x:
            c_str = ISO8601_TO_CTIME_DICT.get(x, x)
            c_list.append(c_str)
            # log.debug(f'Translating datetime format string. ISO8601="{x}" C="{c_str}"')

    c_fmt_str = ''.join(c_list)
    log.debug(f'Translated datetime format string. ISO8601="{iso_str}" C="{c_fmt_str}"')
    return c_fmt_str


def get_dialect(dt_el):
    text_format_el = first(dt_el, './/physical/dataFormat/textFormat')

    # https://docs.python.org/3/library/csv.html#csv.Dialect
    class Dialect(csv.Dialect):
        delimiter = first_str(text_format_el, 'fieldDelimiter', DEFAULT_FIELD_DELIMITER)
        doublequote = True
        escapechar = None
        lineterminator = first_str(text_format_el, 'recordDelimiter', DEFAULT_RECORD_DELIMITER)
        quotechar = first_str(text_format_el, 'quoteCharacter', DEFAULT_QUOTE_CHARACTER)
        quoting = csv.QUOTE_MINIMAL
        skipinitialspace = True
        strict = False

    return Dialect


def get_header_line_count(dt_el):
    text_format_el = first(dt_el, './/physical/dataFormat/textFormat')
    return first_int(text_format_el, 'numHeaderLines', DEFAULT_HEADER_LINE_COUNT)

def get_footer_line_count(dt_el):
    text_format_el = first(dt_el, './/physical/dataFormat/textFormat')
    return first_int(text_format_el, 'numFooterLines', DEFAULT_FOOTER_LINE_COUNT)


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
    """
    default_dt = get_default_begin_end_datetime_range(dt_el)

    type_list = []

    # Iterate over 'attribute', which describes each individual column.
    attr_list = list(dt_el.xpath('.//attributeList/attribute'))

    for col_idx, attr_el in enumerate(attr_list):
        # log.debug()

        col_name = first_str_orig(attr_el, './/attributeName/text()')

        storage_type = first_str(attr_el, './/storageType/text()')
        date_fmt_str = first_str(attr_el, './/measurementScale/dateTime/formatString/text()')
        number_type = first_str(
            attr_el, './/measurementScale/ratio/numericDomain/numberType/text()'
        )
        # numeric_domain = first_str(
        #     attr_el, './/measurementScale/ratio/unit/numericDomain/text()'
        # )
        is_enumerated = has(attr_el, './/nonNumericDomain/enumeratedDomain')
        ratio = first_str(attr_el, './/measurementScale/ratio/text()')
        missing_code_list = multiple_str(attr_el, './/missingValueCode/code/text()')

        dtype_dict = {
            'col_idx': col_idx,
            'col_name': col_name,
            'type_str': 'S_TYPE_UNSUPPORTED',
            'storage_type': storage_type,
            'date_fmt_str': date_fmt_str,
            'c_date_fmt_str': None,
            'number_type': number_type,
            'is_enumerated': is_enumerated,
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
                try_date_time_parse(begin_date_str) if begin_date_str else default_dt.begin_dt
            )
            dtype_dict['end_dt'] = (
                try_date_time_parse(end_date_str) if end_date_str else default_dt.end_dt
            )

        if date_fmt_str:
            dtype_dict['type_str'] = 'TYPE_DATE'
            dtype_dict['date_fmt_str'] = date_fmt_str
            dtype_dict['c_date_fmt_str'] = iso8601_to_c_format(date_fmt_str)
            begin_date_str = first_str(attr_el, './/beginDate/calendarDate/text()')
            end_date_str = first_str(attr_el, './/endDate/calendarDate/text()')
            dtype_dict['begin_dt'] = (
                try_date_time_parse(begin_date_str) if begin_date_str else default_dt.begin_dt
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

        elif is_enumerated:
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


def first_str(el, text_xpath, default_val=None):
    """Apply xpath and, if there is a match, assume that the match is a text node, and
    convert it to str.

    {text_xpath} is an xpath that returns a text node. E.g., `.//text()`.
    """
    res_el = first(el, text_xpath)
    if res_el is None:
        return default_val
    return str(res_el).strip()
    # try:
    # except (ValueError, TypeError, AttributeError):
    #     return default_val


def first_str_orig(el, text_xpath, default_val=None):
    try:
        return str(first(el, text_xpath)).strip()
    except (ValueError, TypeError, AttributeError):
        return default_val


def first_int(el, text_xpath, default_int=None):
    try:
        # int() includes an implicit strip().
        return int(first(el, text_xpath))
    except (ValueError, TypeError, AttributeError):
        return default_int


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
    el = first_str(el, date_xpath)
    if el is not None:
        return str(el).strip() if el else None


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
        begin_dt=try_date_time_parse(begin_str) if begin_str else FALLBACK_BEGIN_DATETIME,
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
        if url == data_url.as_posix():
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

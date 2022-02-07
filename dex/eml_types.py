"""EML type utils

This module works directly with EML XML objects in the lxml.etree domain, and so can
be used without having an `rid`.
"""
import csv
import datetime
import enum
import logging
import re

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


class PandasType(str, enum.Enum):
    FLOAT = 'FLOAT'  # enum.auto()
    INT = 'INT'  # enum.auto()
    CATEGORY = 'CATEGORY'  # enum.auto()
    DATETIME = 'DATETIME'  # enum.auto()
    STRING = 'STRING'  # enum.auto()


PANDAS_TO_FRIENDLY_DICT = {
    PandasType.FLOAT: 'Numeric',
    PandasType.INT: 'Numeric',
    PandasType.CATEGORY: "Categorical",
    PandasType.DATETIME: "Time series",
    PandasType.STRING: "Generic",
}


ISO8601_TO_CTIME_DICT = {
    # This dict was created based on an analysis of the full LTER and EDI corpus of
    # CSV files.
    # https://docs.python.org/3/library/datetime.html#strftime-and-strptime-format-codes
    # -- %a
    # Weekday as locale’s abbreviated name.
    # Sun, Mon, …, Sat (en_US);
    # So, Mo, …, Sa (de_DE)
    'w': '%a',
    'W': '%a',
    'DDD': '%a',
    'WWW': '%a',
    # -- %A
    # Weekday as locale’s full name.
    # Sunday, Monday, …, Saturday (en_US);
    # Sonntag, Montag, …, Samstag (de_DE)
    # -- %w
    # Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
    # 0, 1, …, 6
    'WW': '%w',
    # -- %d
    # Day of the month as a zero-padded decimal number.
    # 01, 02, …, 31
    'DD': '%d',
    'dd': '%d',
    'D': '%d',
    # -- %b
    # Month as locale’s abbreviated name.
    # Jan, Feb, …, Dec (en_US);
    # Jan, Feb, …, Dez (de_DE)
    'Month': '%b',
    'mmm': '%b',
    'MMM': '%b',
    'MON': '%b',
    'mon': '%b',
    # -- %B
    # Month as locale’s full name.
    # January, February, …, December (en_US);
    # Januar, Februar, …, Dezember (de_DE)
    # -- %m
    # Month as a zero-padded decimal number.
    # 01, 02, …, 12
    'MM': '%m',
    # -- %y
    # Year without century as a zero-padded decimal number.
    # 00, 01, …, 99
    'YYY': '%y',
    'YY': '%y',
    'yy': '%y',
    # -- %Y
    # Year with century as a decimal number.
    # 0001, 0002, …, 2013, 2014, …, 9998, 9999
    'YYYY': '%Y',
    'yyyy': '%Y',
    # -- %H
    # Hour (24-hour clock) as a zero-padded decimal number.
    # 00, 01, …, 23
    'HH': '%H',
    'HH24': '%H',
    'hh24': '%H',
    'hh': '%H',
    'h': '%H',
    # -- %I
    # Hour (12-hour clock) as a zero-padded decimal number.
    # 01, 02, …, 12
    # -- %p
    # Locale’s equivalent of either AM or PM.
    # AM, PM (en_US);
    # am, pm (de_DE)
    # -- %M
    # Minute as a zero-padded decimal number.
    # 00, 01, …, 59
    'MI': '%M',
    'mi': '%M',
    'mm': '%M',
    'm': '%M',
    # -- %S
    # Second as a zero-padded decimal number.
    'SS': '%S',
    'ss': '%S',
    's': '%S',
    # -- %f
    # Microsecond as a decimal number, zero-padded to 6 digits.
    # 000000, 000001, …, 999999
    # -- %z
    # UTC offset in the form ±HHMM[SS[.ffffff]] (empty string if the object is naive).
    # (empty), +0000, -0400, +1030, +063415, -030712.345216
    '\+hhmm': '%z',
    # -- %Z
    # Time zone name (empty string if the object is naive).
    # (empty), UTC, GMT
    'UTC': '%Z',
    'GMT': '%Z',
    # -- %j
    # Day of the year as a zero-padded decimal number.
    # 001, 002, …, 366
    # 'DDD': '%j',
    'DDDD': '%j',
    # -- %U
    # Week number of the year (Sunday as the first day of the week) as a zero-padded
    # decimal number. All days in a new year preceding the first Sunday are considered
    # to be in week 0.
    # 00, 01, …, 53
    # -- %W
    # Week number of the year (Monday as the first day of the week) as a zero-padded
    # decimal number. All days in a new year preceding the first Monday are considered
    # to be in week 0.
    # 00, 01, …, 53
    # -- %c
    # Locale’s appropriate date and time representation.
    # Tue Aug 16 21:30:00 1988 (en_US);
    # Di 16 Aug 21:30:00 1988 (de_DE)
    # -- %x
    # Locale’s appropriate date representation.
    # 08/16/88 (None);
    # 08/16/1988 (en_US);
    # 16.08.1988 (de_DE)
    # -- %X
    # Locale’s appropriate time representation.
    # 21:30:00 (en_US);
    # 21:30:00 (de_DE)
    # -- %%
    # A literal '%' character.
    # -- %
    # -- %G
    # ISO 8601 year with century representing the year that contains the greater part of
    # the ISO week (%V).
    # 0001, 0002, …, 2013, 2014, …, 9998, 9999
    # -- %u
    # ISO 8601 weekday as a decimal number where 1 is Monday.
    # 1, 2, …, 7
    # -- %V
    # ISO 8601 week as a decimal number with Monday as the first day of the week. Week
    # 01 is the week containing Jan 4.
    # 01, 02, …, 53
    # Weekday as locale’s full name.
    # Sunday, Monday, …, Saturday (en_US);
    # Weekday as a decimal number, where 0 is Sunday and 6 is Saturday.
    # 0, 1, …, 6
    # Literals to preserve
    'Z': 'Z',
}


def iso8601_to_c_format(iso_str):
    """Convert an ISO8601-style datetime format spec, as used in EML, to a C-style
    format spec, as used by the Python datetime formatting functions.

    Args:
        iso_str (str):
            ISO8601-style datetime format spec from EML

    Returns:
        str: C-style format spec, as used by the Python datetime formatting functions
        None: Returned if 'iso_str' contains one or more sequences that we are unable to
        translate.
    """
    c_list = []
    # We look for keys in longest to shortest order, with key string itself as tie breaker.
    key_list = list(sorted(ISO8601_TO_CTIME_DICT.keys(), key=lambda s: (-len(s), s)))
    rx_str = '|'.join(key_list)
    rx_str = f'({rx_str})'
    for iso_str in re.split(rx_str, iso_str, flags=re.IGNORECASE):
        if iso_str:
            try:
                c_str = ISO8601_TO_CTIME_DICT[iso_str]
            except KeyError:
                if iso_str in list(' _-/,:T{}()[]'):
                    c_str = iso_str
                else:
                    return
            c_list.append(c_str)
            # log.debug(f'Translating datetime format string. ISO8601="{iso_str}" C="{c_str}"')
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
    return first_int(
        dt_el,
        './/physical/dataFormat/textFormat/numHeaderLines/text()',
        DEFAULT_HEADER_LINE_COUNT,
    )


def get_footer_line_count(dt_el):
    return first_int(
        dt_el,
        './/physical/dataFormat/textFormat/numFooterLines/text()',
        DEFAULT_FOOTER_LINE_COUNT,
    )


def get_col_attr_list(dt_el):
    """Get set of column attributes for a CSV file."""
    col_attr_list = []
    # Iterate over 'attribute' elements, one for each column
    attr_list = list(dt_el.xpath('.//attributeList/attribute'))

    for col_idx, attr_el in enumerate(attr_list):
        col_name = first_str_orig(attr_el, './/attributeName/text()')
        pandas_type = derive_pandas_type(attr_el)

        date_fmt_str = None
        c_date_fmt_str = None
        if pandas_type == PandasType.DATETIME:
            date_fmt_str = get_date_fmt_str(attr_el, col_name)
            if date_fmt_str:
                c_date_fmt_str = iso8601_to_c_format(date_fmt_str)
            else:
                pandas_type = PandasType.STRING

        col_attr_list.append(
            dict(
                col_idx=col_idx,
                col_name=col_name,
                pandas_type=pandas_type,
                date_fmt_str=date_fmt_str,
                c_date_fmt_str=c_date_fmt_str,
                missing_code_list=multiple_str(attr_el, './/missingValueCode/code/text()'),
            )
        )

    return col_attr_list


def derive_pandas_type(attr_el):
    """Heuristics to find a Pandas / NumPy type, to use when processing a CSV file that has EML based type declarations.

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
    """
    if has_el(attr_el, 'enumeratedDomain'):
        return PandasType.CATEGORY
    if has_el(attr_el, 'dateTime'):
        return PandasType.DATETIME
    if has_el(attr_el, 'numericDomain'):
        number_type = first_str(attr_el, './/numberType/text()')
        if number_type in ('real', 'natural'):
            return PandasType.FLOAT
        if number_type in ('integer', 'whole'):
            return PandasType.INT
        return PandasType.STRING
    # String is our fallback type.
    return PandasType.STRING
    # if has_el(el, 'nonNumericDomain'):
    #     return 'type_'
    # if has_el(attr_el, 'textDomain'):


def get_date_fmt_str(attr_el, col_name):
    date_fmt_str = first_str(attr_el, './/measurementScale/dateTime/formatString/text()')
    if date_fmt_str:
        return date_fmt_str
    if col_name.upper() == 'YEAR':
        return 'YYYY'


def is_numeric(pandas_type):
    return pandas_type in (PandasType.FLOAT, PandasType.INT)


def has_el(el, el_name):
    """Return True if an element with a given name exists in the branch rooted at el"""
    return True if el.xpath(f'.//{el_name}') else False


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
    return [str(x) for x in el] if el else []


def has(el, xpath):
    return first(el, xpath) is not None


def get_data_table_list(root_el):
    """Return list of dataTable elements in EML doc"""
    if not root_el:
        return []
    return root_el.xpath('.//dataset/dataTable')


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

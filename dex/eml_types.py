"""EML type utils

This module works directly with EML XML objects in the lxml.etree domain, and so can
be used without having an `rid`.

This module should not require cache access and so, should not import `dex.eml_cache`
or `dex.cache`.
"""
import csv
import datetime
import enum
import logging

import dex.eml_date_fmt
import dex.exc

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
    # CUSTOM_DATETIME = 'CUSTOM_DATETIME',


PANDAS_TO_FRIENDLY_DICT = {
    PandasType.FLOAT: 'Rational Numbers',
    PandasType.INT: 'Integer Numbers',
    PandasType.CATEGORY: "Categorical",
    PandasType.DATETIME: "Time Series",
    PandasType.STRING: "Generic",
    # PandasType.CUSTOM_DATETIME: "Time Series",
}


def get_dialect(dt_el):
    text_format_el = first(dt_el, './/physical/dataFormat/textFormat')

    def decode(s):
        """Decode escape sequences.
        E.g., "foo\\nbar" -> "foo\nbar"
        """
        return bytes(s, "utf-8").decode("unicode_escape")

    # https://docs.python.org/3/library/csv.html#csv.Dialect
    class Dialect(csv.Dialect):
        _delimiter = first_str(
            text_format_el,
            'simpleDelimited/fieldDelimiter/text()',
            DEFAULT_FIELD_DELIMITER,
        )
        _lineterminator = first_str(
            text_format_el,
            'recordDelimiter/text()',
            DEFAULT_RECORD_DELIMITER,
        )
        _quotechar = first_str(
            text_format_el,
            'quoteCharacter/text()',
            DEFAULT_QUOTE_CHARACTER,
        )

        delimiter = decode(_delimiter)
        lineterminator = decode(_lineterminator)
        quotechar = decode(_quotechar)

        doublequote = True
        escapechar = None
        quoting = csv.QUOTE_MINIMAL
        skipinitialspace = True
        strict = False

        def __repr__(self):
            return f'<Dialect {self.__class__.__name__} {self.__dict__}>'

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

    used_col_name_set = set()

    for col_idx, attr_el in enumerate(attr_list):
        col_name = first_str_orig(attr_el, './/attributeName/text()')
        pandas_type = derive_pandas_type(attr_el)

        date_fmt_str = None
        date_fmt_dict = None
        if pandas_type == PandasType.DATETIME:
            date_fmt_str = get_date_fmt_str(attr_el, col_name)
            if dex.eml_date_fmt.has_absolute_time(date_fmt_str):
                date_fmt_dict = dex.eml_date_fmt.get_datetime_parser_and_formatter(
                    col_name, date_fmt_str
                )
            if not date_fmt_dict:
                pandas_type = PandasType.STRING

        # Some CSV files have duplicate column names. Because we use column names
        # to reference the columns, we need to resolve those to unique names.
        unique_col_name = col_name
        unique_col_idx = 2
        while unique_col_name in used_col_name_set:
            unique_col_name = f'{col_name} ({unique_col_idx})'
            unique_col_idx += 1
        used_col_name_set.add(unique_col_name)

        missing_code_list = list(
            sorted(set(multiple_str(attr_el, './/missingValueCode/code/text()')))
        )

        col_attr_list.append(
            dict(
                col_idx=col_idx,
                col_name=unique_col_name,
                pandas_type=pandas_type,
                date_fmt_str=date_fmt_str,
                # c_date_fmt_str=c_date_fmt_str,
                date_fmt_dict=date_fmt_dict,
                missing_code_list=missing_code_list,
            )
        )

    return col_attr_list


def derive_pandas_type(attr_el):
    """Heuristics to find a Pandas / NumPy type, to use when processing a CSV file that
    has EML based type declarations.

    Pandas supports a basic set of types, while EML supports much more complex type
    declarations and descriptions. The columns for which we are able to derive a dtype
    are supported with additional functionality in other areas of the app.

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


def get_date_fmt_str(attr_el, col_name):
    return first_str(attr_el, './/measurementScale/dateTime/formatString/text()')


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


def get_data_table_by_package_id(el, pkg_path):
    for dt_el in el.xpath('.//dataset/dataTable'):
        url = first_str(dt_el, './/physical/distribution/online/url/text()')
        # log.debug(f'{url}')
        # log.debug(f'{pkg_path}')
        if url and url.lower().endswith(pkg_path.lower()):
            return dt_el
    raise dex.exc.EMLError(f'Missing DataTable in EML. pkg_path="{pkg_path}"')

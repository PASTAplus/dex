"""PASTA and EML utils"""

import contextlib
import datetime
import io
import logging
import pathlib
import types

import dateutil.parser
import lxml.etree
import pygments
import pygments.formatters
import pygments.lexers

import db
import dex.cache
import dex.csv_tmp
import dex.eml_types
import dex.exc
import dex.pasta

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

EML_STYLE_NAME = 'perldoc'

log = logging.getLogger(__name__)

# Default start and end datetimes used in the UI if the EML lists one or more datetime
# columns, but no datetime ranges.
FALLBACK_START_DATETIME = datetime.datetime(
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


def get_attribute_fragments(rid):
    """Get column descriptions for a given CSV file. The columns are described in EML
    attribute elements.
    """
    ss = io.StringIO()
    dt_el = find_data_table(rid)
    object_name = first_str(dt_el, './/physical/objectName/text()')
    ss.write(f'\n{object_name}\n\n')

    for i, attr_el in enumerate(dt_el.xpath('.//attributeList/attribute')):
        ss.write(pretty_format_fragment(attr_el))

    return ss.getvalue()


def get_profiling_types(rid):
    dt_el = find_data_table(rid)
    return dex.eml_types.get_profiling_types(dt_el)


def has(el, xpath):
    return first(el, xpath) is not None


def first(el, xpath):
    """Return the first match to the xpath if there was a match, else None. Can this be
    done directly in xpath 1.0?
    """
    log.debug(f'first() xpath={xpath} ...')
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
    s = None
    if el:
        s = str(el).upper().strip()
    log.debug(f'first_str() -> {s}')
    return s


def get_iso_date_time(iso_str):
    """Try to parse as ISO8601. Return a string on form 'YYYY-MM-DD' if parsing is
    successful, else None.
    """
    with contextlib.suppress(ValueError, TypeError):
        return dateutil.parser.isoparse(iso_str).strftime('%Y-%m-%d')


# @dex.cache.disk('eml-dt-cols', 'list')
def get_datetime_columns(rid):
    dt_el = find_data_table(rid)
    default_dt = get_default_start_end_datetime_range(rid)
    # attributeList/attribute contains descriptions of the columns in the CSV
    dt_col_list = []
    for i, attr_el in enumerate(dt_el.xpath('.//attributeList/attribute')):
        if first_str(attr_el, './/storageType/text()') not in ('dateTime', 'date'):
            continue
        dt_format_str = first_str(
            attr_el, '//measurementScale/dateTime/formatString/text()'
        )
        begin_dt = get_iso_date_time(
            first_str(attr_el, './/beginDate/calendarDate/text()')
        )
        end_dt = get_iso_date_time(first_str(attr_el, './/endDate/calendarDate/text()'))
        dt_col_list.append(
            dict(
                col_idx=i,
                dt_format_str=dt_format_str,
                begin_dt=begin_dt or default_dt.start_dt,
                end_dt=end_dt or default_dt.end_dt,
            )
        )
    log.debug(f'Found datetime columns: {dt_col_list}')
    return dt_col_list


def get_datetime_columns_as_dict(rid):
    return {d['col_idx']: d for d in get_datetime_columns(rid)}


def get_default_start_end_datetime_range(rid):
    el = get_eml_tree(rid)
    return types.SimpleNamespace(
        start_dt=get_iso_date_time(
            first_str(
                el,
                './/dataset/coverage/temporalCoverage/rangeOfDates/beginDate/calendarDate/text()',
            )
        )
        or FALLBACK_START_DATETIME,
        end_dt=get_iso_date_time(
            first_str(
                el,
                './/dataset/coverage/temporalCoverage/rangeOfDates/endDate/calendarDate/text()',
            )
        )
        or FALLBACK_END_DATETIME,
    )


def find_data_table(rid):
    el = get_eml_tree(rid)
    data_url = db.get_data_url(rid).upper()
    data_url = data_url[data_url.find('/PACKAGE/') :]
    for dt_el in el.xpath('.//dataset/dataTable'):
        url = first_str(dt_el, './/physical/distribution/online/url/text()')
        url = url[url.find('/PACKAGE/') :]
        if url == data_url:
            return dt_el
    raise dex.exc.EMLError(f'Missing DataTable in EML. rid="{rid}"')


# noinspection PyUnresolvedReferences
@dex.cache.disk('eml-highlighted', 'xml')
def get_eml_highlighted_html(rid):
    html_formatter = pygments.formatters.HtmlFormatter(style=EML_STYLE_NAME)
    xml_str = get_eml_xml(rid)
    return (
        pygments.highlight(xml_str, pygments.lexers.XmlLexer(), html_formatter),
        html_formatter.get_style_defs('.highlight'),
    )


@dex.cache.disk('eml', 'xml')
def get_eml_xml(rid):
    root_el = get_eml_tree(rid)
    return pretty_format_fragment(root_el)


M @ dex.cache.disk('eml', 'etree')


def get_eml_tree(rid):
    if isinstance(rid, pathlib.Path):
        eml_path = rid
    else:
        eml_path = dex.csv_tmp.get_eml_path_by_row_id(rid)
    return lxml.etree.parse(eml_path.as_posix())


def pretty_format_fragment(el):
    if not isinstance(el, list):
        el = [el]
    buf = io.BytesIO()
    for e in el:
        buf.write(lxml.etree.tostring(e, pretty_print=True))
    return buf.getvalue().decode('utf-8')

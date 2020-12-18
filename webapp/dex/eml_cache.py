"""PASTA and EML utils"""
import contextlib
import datetime
import io
import types

import dateutil.parser
import lxml.etree
import pygments
import pygments.formatters
import pygments.lexers
import requests
import logging

import db
import dex.pasta
import dex.cache
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

EML_STYLE_NAME = "perldoc"

log = logging.getLogger(__name__)

# Default start and end datetimes used in the UI if the EML lists one or more datetime
# columns, but no datetime ranges.
FALLBACK_START_DATETIME = datetime.datetime(2000, 1, 1,)
FALLBACK_END_DATETIME = datetime.datetime(2040, 1, 1,)


def first(el, xpath):
    """Return the first match to the xpath if there was a match, else None. Can this be
    done directly in xpath 1.0?
    """
    log.debug(f"first() xpath={xpath} ...")
    res_el = el.xpath(f"({xpath})[1]")
    try:
        res_str = res_el[0]
    except IndexError:
        res_str = None
    log.debug(f"first() -> {res_str}")
    return res_str


def get_iso_date_time(iso_str):
    """Try to parse as ISO8601. Return a string on form 'YYYY-MM-DD' if parsing is
    successful, else None.
    """
    with contextlib.suppress(ValueError, TypeError):
        return dateutil.parser.isoparse(iso_str).strftime("%Y-%m-%d")


# @dex.cache.disk('eml-dt-cols', 'list')
def get_datetime_columns(rid):
    data_table_el = _find_data_table(rid)
    default_dt = get_default_start_end_datetime_range(rid)
    # attributeList/attribute contains descriptions of the columns in the CSV
    dt_col_list = []
    for i, attr_el in enumerate(
        data_table_el.xpath(".//attributeList/attribute")
    ):
        if first(attr_el, ".//storageType/text()") not in ("dateTime", "date"):
            continue
        dt_format_str = first(
            attr_el, "//measurementScale/dateTime/formatString/text()"
        )
        begin_dt = get_iso_date_time(
            first(attr_el, ".//beginDate/calendarDate/text()")
        )
        end_dt = get_iso_date_time(
            first(attr_el, ".//endDate/calendarDate/text()")
        )
        dt_col_list.append(
            dict(
                col_idx=i,
                dt_format_str=dt_format_str,
                begin_dt=begin_dt or default_dt.start_dt,
                end_dt=end_dt or default_dt.end_dt,
            )
        )
    log.debug(f"Found datetime columns: {dt_col_list}")
    return dt_col_list


def get_datetime_columns_as_dict(rid):
    return {d["col_idx"]: d for d in get_datetime_columns(rid)}


def get_default_start_end_datetime_range(rid):
    el = get_eml_tree(rid)
    return types.SimpleNamespace(
        start_dt=get_iso_date_time(
            first(
                el,
                ".//dataset/coverage/temporalCoverage/rangeOfDates/beginDate/calendarDate/text()",
            )
        )
        or FALLBACK_START_DATETIME,
        end_dt=get_iso_date_time(
            first(
                el,
                ".//dataset/coverage/temporalCoverage/rangeOfDates/endDate/calendarDate/text()",
            )
        )
        or FALLBACK_END_DATETIME,
    )


def _find_data_table(rid):
    el = get_eml_tree(rid)
    for data_table_el in el.xpath(".//dataTable"):
        data_url = data_table_el.xpath(
            ".//physical//distribution/online/url/text()"
        )
        if not data_url:
            continue
        rid = dex.pasta.parse_pasta_data_url(data_url[0])
        if rid == rid:
            return data_table_el
    raise dex.util.EMLError(f'Missing DataTable in EML. rid="{rid}"')


# noinspection PyUnresolvedReferences
@dex.cache.disk("eml-highlighted", "xml")
def get_eml_highlighted_html(rid):
    html_formatter = pygments.formatters.HtmlFormatter(style=EML_STYLE_NAME)
    xml_str = get_eml_xml(rid)
    return (
        pygments.highlight(
            xml_str, pygments.lexers.XmlLexer(), html_formatter
        ),
        html_formatter.get_style_defs(".highlight"),
    )


@dex.cache.disk("eml", "xml")
def get_eml_xml(rid):
    root_el = get_eml_tree(rid)
    return pretty_format_fragment(root_el)


@dex.cache.disk("eml", "etree")
def get_eml_tree(rid):
    data_url = db.get_data_url(rid)
    eml_url = dex.pasta.pasta_data_to_eml_url(data_url)
    response = requests.get(eml_url)
    response.raise_for_status()
    return lxml.etree.parse(io.BytesIO(response.text.encode("utf-8")))


def pretty_print_fragment(el):
    print(f"\n{pretty_format_fragment(el).strip()}\n")


def pretty_format_fragment(el):
    if not isinstance(el, list):
        el = [el]
    buf = io.BytesIO()
    for e in el:
        buf.write(lxml.etree.tostring(e, pretty_print=True))
    return buf.getvalue().decode("utf-8")


# def build_eml_dict(self):
#     total_count = sum(1 for _ in EML_DIR_PATH.iterdir())
#     all_eml_dict = {}
#
#     for i, eml_path in enumerate(EML_DIR_PATH.iterdir()):
#         print(i)
#         try:
#             with eml_path.open('rb') as f:
#                 tree = etree.parse(f)
#         except lxml.etree.Error as e:
#             # print(str(e))
#             continue
#         except IOError as e:
#             # print(str(e))
#             continue
#
#         self.extract_eml(tree)
#
# def extract_eml(self, xml):
#     """Extract dict of rid to filename from a single EML doc."""
#     self.count('__files')
#     for dt_el in xml.xpath('.//dataTable'):
#         self.count('__datatables')
#
#         attr_list = dt_el.xpath('.//attribute')
#         for attr in attr_list:
#             self.count('__columns')
#
#             unit_list = attr.xpath('.//unit')
#             for unit in unit_list:
#                 std_list = unit.xpath('.//standardUnit')
#                 for std in std_list:
#                     self.count(f'unit {std.text}')
#
#             dt_list = attr.xpath('.//dateTime')
#             for dt in dt_list:
#                 fmt_list = dt.xpath('.//formatString')
#                 for fmt in fmt_list:
#                     self.count(f'dateTime {fmt.text}')
#
# def count(self, k):
#     print(k)
#     self.count_dict[k] += 1
#
#
#     for physical_el in xml.xpath('//physical', is_required=False):
#         log.info("Found <physical>")
#         # log.debug(d1_common.xml.etree_to_pretty_xml(physical_el))
#
#         rid = self.extract_text(physical_el, 'distribution/online/url')
#         if rid is None:
#             # Event was recorded in extract_text()
#             continue
#
#         object_name = self.extract_text(physical_el, 'objectName')
#         if object_name is None:
#             # Event was recorded in extract_text()
#             continue
#
#         if not rid.startswith('https://pasta.lternet.edu/'):
#             log.info(
#                 'Ignored rid not starting with "https://pasta.lternet.edu/"'
#             )
#             continue
#
#         physical_dict.setdefault(rid, []).append(object_name)
#
#     return ret_dict
#
# # def get_package_id(self, xml):
# #     package_id = xml.get_element('.')[0].attrib['packageId']
# #     package_tup = package_id.split('.')
# #     if len(package_tup) != 3:
# #         log.info(f'Invalid packageId. package_id="{package_id}"')
# #         return PackageId(package_id, '', '')
# #
# #     return PackageId(*package_tup)
#
# # def extract_text(self, physical_el, xpath):
# #     el = physical_el.find(xpath)
# #     # ElementTree has non-standard behavior when evaluating elements in a boolean
# #     # context. True if it has children, False otherwise.
# #     if el is None:
# #         log.info(f"No element at {xpath}", xpath=xpath)
# #         return
# #     log.info(f"Found element at {xpath}", xpath=xpath)
# #     v = el.text
# #     if v is None:
# #         log.info(f"No text in element at {xpath}", xpath=xpath)
# #     return v
#
#
# if __name__ == "__main__":
# main()
#

"""Create an EML document that is customized for a subset of the CSV
"""

import datetime
import logging
import pathlib
import pprint

import lxml.etree

import dex.db

# import dex.cache
import dex.csv_parser
import dex.obj_bytes
import dex.eml_types
import dex.eml_cache
import dex.exc
import dex.pasta
import dex.util

log = logging.getLogger(__name__)


# def create_eml(rid, filter_dict):
#     """
#     - For any column that is removed, remove the corresponding attribute branch
#
#     - Copy complete source EML, then remove DataTable elements other than the one used for the CSV.
#     - Add or update: Number of rows, size, checksum (authentication element)
#     - Mark will provide a template for provenance info (pointing back to the source dataset)
#
#     Args:
#         rid:
#         filter_dict:
#
#     Returns:
#         EML document
#     """
#     eml_el = dex.eml_cache.get_eml_etree(rid)
#     entity_tup = dex.db.get_entity(rid)
#     pkg_id = dex.pasta.get_pkg_id_as_url(entity_tup)
#     dt_el = dex.eml_types.get_data_table_by_package_id(eml_el, pkg_path)


def create_subset_eml(
    rid,
    row_count,
    byte_count,
    md5_checksum,
    col_list,
):
    """Create EML doc representing a CSV subset"""
    eml_el = dex.eml_cache.get_eml_etree(rid)
    entity_tup = dex.db.get_entity(rid)
    pkg_id = dex.pasta.get_pkg_id_as_url(entity_tup)
    _subset_eml(eml_el, pkg_id, row_count, byte_count, md5_checksum, col_list)
    return dex.util.get_etree_as_pretty_printed_xml(eml_el)


def _subset_eml(
    eml_el,
    pkg_id,
    row_count,
    byte_count,
    md5_checksum,
    col_list,
):
    """Inline modify EML etree to match CSV subset"""
    _prune_data_table(eml_el, pkg_id)
    physical_el = dex.eml_types.first(eml_el, './/physical')
    assert isinstance(physical_el, lxml.etree._Element)
    _prune_distribution(physical_el)
    _prune_columns(eml_el, col_list)
    _set_row_count(physical_el, row_count)
    _set_byte_count(physical_el, byte_count)
    _set_md5_checksum(physical_el, md5_checksum)


def _prune_data_table(eml_el, pkg_path):
    """Remove .//dataset/dataTable branches from tree, except for the dataTable
    containing a .//physical/distribution/online/url/text() with the value of
    'pkg_path'.
    """
    keep_dt_el = dex.eml_types.get_data_table_by_package_id(eml_el, pkg_path)
    dataset_el = dex.eml_types.first(eml_el, './/dataset')
    assert dataset_el is not None
    assert isinstance(dataset_el, lxml.etree._Element)
    for dt_el in dataset_el.xpath('.//dataTable'):
        if dt_el != keep_dt_el:
            dataset_el.remove(dt_el)


def _prune_distribution(physical_el):
    for dist_el in physical_el.xpath(".//distribution"):
        physical_el.remove(dist_el)


def _prune_columns(eml_el, col_list):
    attr_list_el = dex.eml_types.first(eml_el, './/attributeList')
    for i, attr_el in enumerate(attr_list_el.xpath(".//attribute")):
        if not col_list[i]:
            attr_list_el.remove(attr_el)


def _set_row_count(physical_el, row_count):
    """Add or replace size with unit 'rows' in physical element

    <physical>
      <size unit="rows">1234</size>
    """
    for size_el in physical_el.xpath(".//size[@unit='rows']"):
        physical_el.remove(size_el)

    # xml.etree.ElementTree.SubElement(parent, tag, attrib={}, **extra)

    size_el = lxml.etree.Element("size")
    size_el.set('unit', 'rows')
    size_el.text = str(row_count)

    physical_el.append(size_el)


def _set_byte_count(physical_el, byte_count):
    """Add or replace size with unit 'bytes' in physical element

    <physical>
      <size unit="bytes">1234</size>
    """
    for size_el in physical_el.xpath(".//size[@unit='bytes']"):
        physical_el.remove(size_el)

    size_el = lxml.etree.Element("size")
    size_el.set('unit', 'bytes')
    size_el.text = str(byte_count)

    physical_el.append(size_el)


def _set_md5_checksum(physical_el, md5_str):
    """
    <physical>
      <authentication method="MD5">f0e60724d51ae3cf62b5ade2bac416b1</authentication>
    """

    for auth_el in physical_el.xpath('authentication'):
        physical_el.remove(auth_el)

    auth_el = lxml.etree.Element("authentication")
    auth_el.set('method', 'MD5')
    auth_el.text = md5_str

    physical_el.append(auth_el)

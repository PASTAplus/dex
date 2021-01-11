#!/usr/bin/env python

import collections
import io
import logging
import pathlib
import pprint

import lxml
from lxml import etree

log = logging.getLogger(__name__)

PackageId = collections.namedtuple("packageId", ("scope", "id", "version"))

EML_DIR_PATH = pathlib.Path("~/hdd/eml").expanduser()


def main():
    c = Command()
    c.run()


class Command:
    def __init__(self):
        self.unit_count = 0
        self.no_unit_count = 0
        self.count_dict = collections.defaultdict(lambda: 0)

    def run(self):
        self.analyze_eml()
        pprint.pprint(self.count_dict)
        # print(f'unit_count={self.unit_count}')
        # print(f'no_unit_count={self.no_unit_count}')

    def analyze_eml(self):
        all_eml_dict = self.build_eml_dict()
        # cmdbase.util.misc.save_json(settings.EML_DICT, all_eml_dict)

    def build_eml_dict(self):
        total_count = sum(1 for _ in EML_DIR_PATH.iterdir())
        all_eml_dict = {}

        for i, eml_path in enumerate(EML_DIR_PATH.iterdir()):
            try:
                with eml_path.open("rb") as f:
                    tree = etree.parse(f)
            except lxml.etree.Error as e:
                # print(str(e))
                continue
            except IOError as e:
                # print(str(e))
                continue

            self.extract_eml(tree)

    def extract_eml(self, xml):
        print(xml.xpath(".//entityName/text()"))

    def extract_eml0(self, xml):
        """Extract dict of row_id to filename from a single EML doc."""
        self.count("__files")
        for dt_el in xml.xpath(".//dataTable"):
            self.count("__datatables")

            attr_list = dt_el.xpath(".//attribute")
            for attr in attr_list:
                self.count("__columns")

                unit_list = attr.xpath(".//unit")
                for unit in unit_list:
                    std_list = unit.xpath(".//standardUnit")
                    for std in std_list:
                        self.count(f"unit {std.text}")

                dt_list = attr.xpath(".//dateTime")
                for dt in dt_list:
                    fmt_list = dt.xpath(".//formatString")
                    for fmt in fmt_list:
                        self.count(f"dateTime {fmt.text}")

    def count(self, k):
        self.count_dict[k] += 1

    def pretty_print_fragment(self, el):
        if not isinstance(el, list):
            el = [el]
        for e in el:
            buf = io.BytesIO()
            buf.write(etree.tostring(e, pretty_print=True))
            print(buf.getvalue().decode("utf-8"))

        # lxml.to_xml(el)
        #
        # print(file)

        # physical_dict = {}
        #
        # ret_dict = {}
        # ret_dict = {'physical': physical_dict, 'package_id': self.get_package_id(xml)}
        #
        # for physical_el in xml.xpath('//physical', is_required=False):
        #     log.info("Found <physical>")
        #     # log.debug(d1_common.xml.etree_to_pretty_xml(physical_el))
        #
        #     row_id = self.extract_text(physical_el, 'distribution/online/url')
        #     if row_id is None:
        #         # Event was recorded in extract_text()
        #         continue
        #
        #     object_name = self.extract_text(physical_el, 'objectName')
        #     if object_name is None:
        #         # Event was recorded in extract_text()
        #         continue
        #
        #     if not row_id.startswith('https://pasta.lternet.edu/'):
        #         log.info(
        #             'Ignored row_id not starting with "https://pasta.lternet.edu/"'
        #         )
        #         continue
        #
        #     physical_dict.setdefault(row_id, []).append(object_name)
        #
        # return ret_dict

    # def get_package_id(self, xml):
    #     package_id = xml.get_element('.')[0].attrib['packageId']
    #     package_tup = package_id.split('.')
    #     if len(package_tup) != 3:
    #         log.info(f'Invalid packageId. package_id="{package_id}"')
    #         return PackageId(package_id, '', '')
    #
    #     return PackageId(*package_tup)

    # def extract_text(self, physical_el, xpath):
    #     el = physical_el.find(xpath)
    #     # ElementTree has non-standard behavior when evaluating elements in a boolean
    #     # context. True if it has children, False otherwise.
    #     if el is None:
    #         log.info(f"No element at {xpath}", xpath=xpath)
    #         return
    #     log.info(f"Found element at {xpath}", xpath=xpath)
    #     v = el.text
    #     if v is None:
    #         log.info(f"No text in element at {xpath}", xpath=xpath)
    #     return v


if __name__ == "__main__":
    main()

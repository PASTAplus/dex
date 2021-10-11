import csv
import pprint
import dex.util

from flask import current_app as app

import dex.csv_parser

import logging

log = logging.getLogger(__name__)

p = lambda *a, **kw: dex.util.logpp(*a, **kw, logger=log.debug)

# Dialects
# See also: pandas/tests/io/parser/test_dialect.py


# 1
DATA_URL_1 = 'https://pasta-d.lternet.edu/package/data/eml/knb-lter-cce/72/2/f12ac76be131821d245316854f7ddf44'


class Dialect1(csv.excel):
    delimiter = ','
    doublequote = True
    escapechar = None
    lineterminator = '\r\n'
    quotechar = '"'
    quoting = 0
    skipinitialspace = False


def test_0100(rid, csv_path):
    """get_dialect()"""
    dialect = dex.csv_parser.get_dialect(csv_path)
    assert dex.csv_parser.get_dialect_as_dict(dialect) == dex.csv_parser.get_dialect_as_dict(
        Dialect1
    )


def test_1000(rid, tmp_cache):
    """get_parsed_csv()"""
    header_line_count = 0
    # parser_dict = {}
    # df = dex.csv_parser.get_parsed_csv(rid, header_line_count, parser_dict, Dialect1)
    parser_dict = {
        'studyName': None,
        'Datetime GMT': None,
        'Cycle': None,
        'Event Number': None,
        'Cast Number': None,
        'Bottle Number': None,
        'Depth (m)': None,
        'Sample Name': None,
        'Total Chlorophyll a (µg/l)': None,
        'Chlorophyll a1 (µg/l)': None,
        'Chlorophyll a2 (µg/l)': None,
        'Chlorophyllide a (µg/l)': None,
        'Chlorophyll b (µg/l)': None,
        'Chlorophyll c (µg/l)': None,
        'Chlorophyll c 1 (µg/l)': None,
        'Chlorophyll c 2 (µg/l)': None,
        'Chlorophyll c 3 (µg/l)': None,
        'Peridinin (µg/l)': None,
        "19'-butanoyloxyfucoxanthin (µg/l)": None,
        'Fucoxanthin (µg/l)': None,
        'Neoxanthin (µg/l)': None,
        'Prasinoxanthin (µg/l)': None,
        'Violaxanthin (µg/l)': None,
        "19'-hexanoyloxyfucoxanthin (µg/l)": None,
        'Diadinoxanthin (µg/l)': None,
        'Alloxanthin (µg/l)': None,
        'Diatoxanthin (µg/l)': None,
        'Zeaxanthin (µg/l)': None,
        'Pheophorbide a (µg/l)': None,
    }

    derived_dict = dex.csv_parser.get_derived_dtypes_from_eml(rid)
    p(derived_dict, 'derived_dict')
    parser_dict = dex.csv_parser.get_parser_dict(derived_dict)
    p(parser_dict, 'parser_dict')
    parser_func_dict = {derived_dict[k]['col_name']: d['fn'] for k, d in parser_dict.items()}
    p(parser_func_dict, 'parser_func_dict')
    df = dex.csv_parser.get_parsed_csv(rid, header_line_count, parser_func_dict, Dialect1)
    df.info()

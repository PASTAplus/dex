import lxml.etree

import dex.cache
import dex.csv_parser
import dex.csv_tmp
import dex.eml_cache
# def test_1000(config):
#     assert not config['DISK_CACHE_ENABLED']
import util


def test_1001(rid):
    assert not dex.cache.is_cached(rid, 'html', 'df')
    csv_path = dex.csv_tmp.get_data_path_by_row_id(rid)
    csv_path.exists()


def test_1003(rid):
    root_el = dex.eml_cache.get_eml_etree(rid)
    assert isinstance(root_el, lxml.etree._ElementTree)


def test_1010(rid):
    d = dex.csv_parser.get_parsed_csv_with_context(rid)  # , 'html', 'df')
    util.logpp(d)


def test_1020(rid):
    dt_el = dex.eml_cache.get_data_table(rid)


# print(csv_path)


# p = docs_path / '1.csv'
# main.sample_get(p.as_posix())
# print(p)

# CSV_ROOT_DIR

# def test_1010():
#     assert c.is_cached(rid, key, obj_type)
#
# def test_1020():
#     rid = db.add_entity(data_url)
#     rid, key, obj_type

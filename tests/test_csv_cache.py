import lxml.etree

import dex.cache
import dex.csv_parser
import dex.obj_bytes
import dex.eml_cache

# def test_1000(config):
#     assert not config['DISK_CACHE_ENABLED']
import dex.util


def test_1001(rid):
    assert not dex.cache.is_cached(rid, 'html', 'df')
    csv_stream = dex.obj_bytes.open_csv(rid)
    csv_stream.exists()


def test_1003(rid):
    root_el = dex.eml_cache.get_eml_etree(rid)
    assert isinstance(root_el, lxml.etree._ElementTree)


def test_1010(rid):
    d = dex.csv_parser.get_parsed_csv_with_context(rid)  # , 'html', 'df')
    dex.util.logpp(d)


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
#     rid = dex.db.add_entity(data_url)
#     rid, key, obj_type

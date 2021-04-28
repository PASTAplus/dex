"""Unit tests using pytest and pytest-flask
https://pytest-flask.readthedocs.io/en/latest/
"""
import csv
import pathlib
import pytest

import flask

# from flask import current_app as app

# import util
import db as _db
import dex.pasta

# 1
DATA_URL_1 = 'https://pasta-d.lternet.edu/package/data/eml/knb-lter-cce/72/2/f12ac76be131821d245316854f7ddf44'
class Dialect1(csv.excel):
    delimiter=','
    doublequote=True
    escapechar=None
    lineterminator='\r\n'
    quotechar='"'
    quoting=0
    skipinitialspace=False


DEX_ROOT = pathlib.Path(__file__).parent.resolve()
WEBAPP_ROOT = DEX_ROOT / 'webapp'
TEST_DOCS = DEX_ROOT / 'tests/test_docs'


@pytest.fixture(autouse=True)
def app():
    app = flask.Flask(
        __name__,
        # instance_path=DEX_ROOT.as_posix(),
        # instance_relative_config['T']rue,
        static_url_path="/static/",
        static_folder=(WEBAPP_ROOT / "static").resolve().as_posix(),
        template_folder=(WEBAPP_ROOT / "templates").resolve().as_posix(),
    )
    app.config.from_object("config")
    # util.logpp(app.config, 'app.config', sort_keys=True)
    app.debug = app.config["FLASK_DEBUG"]
    # app.config = N(app.config)
    return app


# @pytest.fixture(autouse=True, scope='session')
# def util():
#     return util


@pytest.fixture()
def disable_cache(config):
    config['DISK_CACHE_ENABLED'] = False


@pytest.fixture()
def enable_cache(config):
    config['DISK_CACHE_ENABLED'] = True


@pytest.fixture
def tmp_cache(config, tmpdir):
    """Move the cache to a temporary dir that is empty at the start of the test and
    deleted after the test."""
    config['CACHE_ROOT_DIR'] = pathlib.Path(tmpdir)
    return config['CACHE_ROOT_DIR']


@pytest.fixture
def tmpdir(config, tmpdir):
    return pathlib.Path(tmpdir)


@pytest.fixture(autouse=True)
def db(config, tmpdir):
    config['SQLITE_PATH'] = tmpdir / 'sqlite.db'
    _db.init_db()
    # rm -rf ../cache
    # rm -rf ../dex-cache
    # rm sqlite.db
    # sqlite3 < schema.sql sqlite.db


@pytest.fixture
def docs_path():
    return TEST_DOCS


@pytest.fixture(autouse=True)
def csv_root(docs_path, config):
    """Set the root path for the CSV samples to the tests/test_docs dir, and return the
    dir.
    """
    config['CSV_ROOT_DIR'] = docs_path
    return config['CSV_ROOT_DIR']


@pytest.fixture
def rid(enable_cache, csv_root):
    # docs_path / '1.csv'
    rid = _db.add_entity(DATA_URL_1)
    return rid


@pytest.fixture
def csv_path(entity_tup):
    return dex.pasta.get_data_path(entity_tup)


@pytest.fixture
def csv_dialect(csv_path):
    return dex.csv_parser.get_csv_dialect(csv_path)


@pytest.fixture
def entity_tup():
    return dex.pasta.get_entity_tup(DATA_URL_1)


# http://127.0.0.1:5000/sample/
# https%3A%2F%2Fpasta-d.lternet.edu%2Fpackage%2Fdata%2Feml%2Fknb-lter-cce%2F72%2F2%2Ff12ac76be131821d245316854f7ddf44
# https://pasta-d.lternet.edu/package/data/eml/knb-lter-cce/72/2/f12ac76be131821d245316854f7ddf44

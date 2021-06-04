"""Unit tests using pytest and pytest-flask
https://pytest-flask.readthedocs.io/en/latest/
"""
import csv
import pathlib

import flask

# import util
import pandas as pd
import pytest

import db as _db
import dex.pasta

from flask import current_app as app

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
def dialect(csv_path):
    return dex.csv_parser.get_dialect(csv_path)


@pytest.fixture
def entity_tup():
    return dex.pasta.get_entity_tup(DATA_URL_1)


@pytest.fixture
def df_random():
    """DataFrame with randomized content"""
    df = pd.util.testing.makeDataFrame()
    df.head()
    return df


@pytest.fixture
def df_missing():
    """DataFrame with missing values"""
    df = pd.util.testing.makeMissingDataframe()
    df.head()
    return df


@pytest.fixture
def df_time():
    """DataFrame with time series"""
    df = pd.util.testing.makeTimeDataFrame()
    df.head()
    return df


@pytest.fixture
def df_mixed():
    """DataFrame with mixed data"""
    df = pd.util.testing.makeMixedDataFrame()
    df.head()
    return df


@pytest.fixture
def df_periodical():
    """DataFrame with periodical data"""
    df = pd.util.testing.makePeriodFrame()
    df.head()
    return df


# @pytest.fixture(scope='function', autouse=True)
# def client(config, test_client, client, app_context):
#     # db_fd, app.config['DATABASE'] = tempfile.mkstemp()
#     try:
#         config['TESTING'] = True
#         config["CSV_ROOT_DIR"] = '/tmp/test/root'
#
#         with test_client() as client:
#             with app_context():
#                 #     app.init_db()
#                 yield client
#     finally:
#         # os.close(db_fd)
#         # os.unlink(app.config['DATABASE'])
#         pass

"""Unit tests using pytest and pytest-flask
https://pytest-flask.readthedocs.io/en/latest/
"""
import csv
import logging
import os
import pathlib
import tempfile

import pandas as pd
import pytest


import dex.db
import dex.main
import dex.main
import dex.pasta

DATA_URL_1 = 'https://pasta-d.lternet.edu/package/data/eml/knb-lter-cce/72/2/f12ac76be131821d245316854f7ddf44'

DEX_ROOT = pathlib.Path(__file__).parent.resolve()
TEST_DOCS = DEX_ROOT / 'tests/test_docs'


class Dialect1(csv.excel):
    delimiter = ','
    doublequote = True
    escapechar = None
    lineterminator = '\r\n'
    quotechar = '"'
    quoting = 0
    skipinitialspace = False


logging.getLogger('matplotlib').setLevel(logging.ERROR)


@pytest.fixture
def app():
    app = dex.main.create_app()
    return app


@pytest.fixture
def app_context(app):
    """App context with initialized temporary DB."""
    db_fd, db_path = tempfile.mkstemp()
    app.config['SQLITE_PATH'] = pathlib.Path(db_path)
    try:
        with app.app_context() as ctx:
            dex.db.init_db()
            yield ctx
    finally:
        os.close(db_fd)
        os.unlink(app.config['SQLITE_PATH'])


# @pytest.fixture(scope='function', autouse=True)
# @pytest.fixture
# def client(app, app_context):
#     with app.test_client() as client:
#         yield client
#


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
    rid = dex.db.add_entity(DATA_URL_1)
    return rid


@pytest.fixture
def csv_path(entity_tup):
    return dex.pasta.get_data_path(entity_tup)


@pytest.fixture
def dialect(csv_path):
    return dex.csv_parser.get_dialect(csv_path)


@pytest.fixture
def entity_tup():
    return dex.pasta.get_entity_by_data_url(DATA_URL_1)


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


# DB and client


@pytest.fixture(scope='function', autouse=True)
def expose_errors(config):
    """Disable automatic error handling during request."""
    config['TESTING'] = True

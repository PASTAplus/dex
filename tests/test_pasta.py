"""Test the dex.pasta module.
"""

import pathlib

import dex.pasta
import dex.pasta

DATA_URL_1 = (
    'https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1'
    '/0be92831cb9e173a828416a954778598'
)
FILE_URI_1 = (
    'file:///tmp/test/root/knb-lter-ble.9.1/0be92831cb9e173a828416a954778598'
)
EML_URL_1 = 'https://pasta-d.lternet.edu/package/metadata/eml/knb-lter-ble/9/1'
PKG_ID = 'knb-lter-ble.9.1.0be92831cb9e173a828416a954778598'
DATA_PATH_1 = pathlib.Path(
    '/tmp/test/root/knb-lter-ble.9.1/0be92831cb9e173a828416a954778598'
)
ENTITY_TUP_1 = dex.pasta.EntityTup(
    data_url=(
        'https://pasta-d.lternet.edu/package/data/eml/knb-lter-ble/9/1/'
        '0be92831cb9e173a828416a954778598'
    ),
    base_url='https://pasta-d.lternet.edu/package',
    scope_str='knb-lter-ble',
    identifier_int=9,
    version_int=1,
    entity_str='0be92831cb9e173a828416a954778598',
)
ENTITY_TUP_2 = dex.pasta.EntityTup(
    data_url=(
        'file:///tmp/test/root/knb-lter-ble.9.1/'
        '0be92831cb9e173a828416a954778598'
    ),
    base_url='file:///tmp/test/root/',
    scope_str='knb-lter-ble',
    identifier_int=9,
    version_int=1,
    entity_str='0be92831cb9e173a828416a954778598',
)


def test_1000():
    assert dex.pasta.get_eml_url(ENTITY_TUP_1) == EML_URL_1


def test_1010():
    assert dex.pasta.get_data_url(ENTITY_TUP_1) == DATA_URL_1


def test_1020():
    assert dex.pasta.get_data_path(ENTITY_TUP_1) == DATA_PATH_1


def test_1030():
    assert dex.pasta.get_entity_tup(DATA_URL_1) == ENTITY_TUP_1


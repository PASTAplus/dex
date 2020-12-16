import os
import pathlib
import shutil
import uuid

import pendulum
import pytest

from dex.gc import dispose
from webapp.config import Config

FILE_NAME = "LTER.NIN.DWS.csv"
PASTA_PATH = "pasta.lternet.edu/package/data/eml/knb-lter-nin/1/1"

UNIQUE1 = str(uuid.uuid4())
PATH_SPEC1 = f"{Config.ROOT_DIR}/{UNIQUE1}/{PASTA_PATH}"
file_path1 = f"{PATH_SPEC1}/{FILE_NAME}"

UNIQUE2 = str(uuid.uuid4())
PATH_SPEC2 = f"{Config.ROOT_DIR}/{UNIQUE2}/{PASTA_PATH}"
file_path2 = f"{PATH_SPEC2}/{FILE_NAME}"


@pytest.fixture()
def conf_dirs():
    pathlib.Path(f"{Config.ROOT_DIR}/static").mkdir(parents=True, exist_ok=True)

    # Fresh data path
    pathlib.Path(f"{PATH_SPEC1}").mkdir(parents=True, exist_ok=True)
    shutil.copy(f"./data/{FILE_NAME}", f"{file_path1}")
    assert pathlib.Path(file_path1).exists()

    # Stale data path
    pathlib.Path(f"{PATH_SPEC2}").mkdir(parents=True, exist_ok=True)
    shutil.copy(f"./data/{FILE_NAME}", f"{file_path2}")
    assert pathlib.Path(file_path2).exists()
    now = pendulum.now()
    mod_time = now.subtract(minutes=Config.STALE + 5).timestamp()
    os.utime(f"{Config.ROOT_DIR}/{UNIQUE2}", times=(mod_time, mod_time))

    yield

    shutil.rmtree(f"{Config.ROOT_DIR}/{UNIQUE1}", ignore_errors=True)
    shutil.rmtree(f"{Config.ROOT_DIR}/{UNIQUE2}", ignore_errors=True)


def test_gc(conf_dirs):
    dispose()
    assert pathlib.Path(file_path1).exists()
    assert pathlib.Path(file_path2).exists() is False

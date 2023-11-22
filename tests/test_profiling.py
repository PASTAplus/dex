import logging
import pathlib
import pprint
import random
import subprocess

import _pytest.pathlib
import numpy as np
import pandas as pd
import ydata_profiling

import dex.main

log = logging.getLogger(__name__)

dex_path = _pytest.pathlib.Path(__file__).resolve().parents[2]
yml_config_path = (dex_path / 'profiling_config.yml').as_posix()


def _generate_and_open_report(df):
    report_tree = ydata_profiling.ProfileReport(
        df,
        # config_file=arg_dict['yml_config_path'],
        # dark_mode=True,
        infer_dtypes=False,
    )
    html_str = report_tree.to_html()
    html_path = pathlib.Path('test_prof.html')
    html_path.write_text(html_str)
    subprocess.run(['firefox', html_path.as_posix()])


def test_1000(client):
    """Start with a blank database."""
    flask_app = dex.main.create_app()
    with flask_app.test_client() as test_client:
        pprint.pp(flask_app.url_map)
        response = test_client.get('/dex/')
        assert response.status_code == 200

    # rv = client.get('/')
    # print(client.application)
    # assert b'No entries here so far' in rv.data


def test_1010(df_missing):
    """"""
    df: pd.DataFrame = df_missing
    # ctx = dex.csv_parser.get_parsed_csv_with_context(arg_dict['rid'])
    print(df)
    # df['A'] = df['A'].round(0)
    # df['A'] = df['A'].astype("category")
    # df['B'] = df['B'].astype("category")
    del df['B']
    del df['C']
    del df['D']

    # df['A'][3] = 999
    # df['A'][3] = np.nan
    # df.transform(lambda x: np.nan if x is None or x in (999, ) else x)

    df.applymap(lambda x: x.fillna(None) if x in (999,) else x)

    print(df)

    _generate_and_open_report(df)


def test_1020():
    """Always interpreted as numeric by Pandas Profiling:
    - Any sequence of INTEGERS, with any number of np.nan
    """
    random.seed(123)
    a = [random.randint(0, 1000) for _ in range(10)]
    for i in range(0, 3):
        a[i] = np.nan
    print(a)
    df = pd.DataFrame({'a': a})
    print(df)

    _generate_and_open_report(df)


def test_1030():
    """Always interpreted as numeric by Pandas Profiling:
    - Any sequence of FLOATS, with any number of np.nan
    """
    random.seed(123)
    a = [random.random() for _ in range(10)]
    a[4] = np.nan

    # for i in range(0, 9):
    #     a[i] = np.nan
    print(a)
    df = pd.DataFrame({'a': a})
    print(df)

    _generate_and_open_report(df)


def test_1040():
    random.seed(123)
    a = ['xyz' for _ in range(10)]
    a[4] = np.nan

    # for i in range(0, 9):
    #     a[i] = np.nan
    print(a)
    df = pd.DataFrame({'a': a})
    print(df)
    print(df.dtypes)

    df = df.astype(float, errors='ignore')
    print(df)

    _generate_and_open_report(df)


def test_a():
    print('AAAAAAAAAAAAAAAAAA')

import logging
import os
import pathlib
import tempfile

import _pytest.pathlib
import pandas_profiling
import pytest
import pprint

import dex.main

log = logging.getLogger(__name__)

dex_path = _pytest.pathlib.Path(__file__).resolve().parents[2]
yml_config_path = (dex_path / 'profiling_config.yml').as_posix()


def test_1000(client):
    """Start with a blank database."""
    flask_app = dex.main.create_app()
    with flask_app.test_client() as test_client:
        pprint.pp(flask_app.url_map)
        response = test_client.get('/dex/')
        assert response.status_code == 200

    # rv = client.get('/')
    # # print(client.application)
    # # print(client.)
    # pprint.pp(rv)
    #
    # # assert b'No entries here so far' in rv.data



# def test_1000(df_missing):
#     """"""
#     df = df_missing
#
#     # ctx = dex.csv_parser.get_parsed_csv_with_context(arg_dict['rid'])
#     print(df)
#
#     df['A'] = df['A'].round(0)
#
#     df['A'] = df['A'].astype("category")
#     df['B'] = df['B'].astype("category")
#
#
#
#     print(df)
#
#
#     # return
#
#     report_tree = pandas_profiling.ProfileReport(
#         df,
#         # config_file=arg_dict['yml_config_path'],
#         # dark_mode=arg_dict['dark_mode'],
#         infer_dtypes=False,
#     )
#
#     html_str = report_tree.to_html()
#     pathlib.Path('test_prof.html').write_text(html_str)

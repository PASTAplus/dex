#!/usr/bin/env python

"""Perform operations on the Dex cache
"""

import argparse
import logging
import pathlib
import sys
import urllib.parse

import flask
import requests

import dex.cache

sys.path.append(pathlib.Path('../webapp').resolve().as_posix())
# print('\n'.join([s.as_posix(), sys.path))

import db

log = logging.getLogger(__name__)


HOST_DICT = {
    'dev': '127.0.0.1:5000',
    'prod': 'https://dex.edirepository.org',
}


def flask_main(_ctx):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        '--env',
        choices=HOST_DICT.keys(),
        default='dev',
        help=', '.join(f'{k}={v}' for k, v in HOST_DICT.items()),
    ),
    parser.add_argument('--debug', help='Debug level logging')
    subparsers = parser.add_subparsers()
    single_parser = subparsers.add_parser('single')
    single_parser.add_argument('rid', help='Row ID')
    all_parser = subparsers.add_parser('all')
    args = parser.parse_args()
    refresh_single(args)


class DexCache:
    def refresh_single(self, args):
        db.get_entity(args.pgk_id_search)
        dex.cache.delete_cache_file(args.rid, 'parsed', 'df')

    def refresh_all(self):
        for row in db.query_db(
            """
            select id, data_url, base_url, scope, identifier, version, entity
            from entity where id = 12
        """,
            tuple(),
            one=False,
        ):
            pass

    def trigger(self, rid):
        data_url = db.get_data_url(rid)
        refresh_fn('sample', data_url)
        for fn in (
            'dex/profile',
            'dex/profile/doc',
            'dex/subset',
            'dex/plot',
            'dex/eml',
        ):
            refresh_fn(fn, rid)

    def refresh_fn(fn, param):
        url = f'https://dex.edirepository.org/{fn}/{param}'
        print(url)
        res = requests.get(url)
        print(res)
        assert res.status_code == 200


if __name__ == '__main__':
    app = flask.Flask(__name__)
    app.config.from_object("config")
    with app.app_context() as ctx:
        sys.exit(flask_main(ctx))

#!/usr/bin/env python

"""Delete all cached objects from the filesystem and
corresponding information from the database.
"""

import argparse
import logging
import sys

import flask

import dex.util

# import requests

# sys.path.append(pathlib.Path('../dex').resolve().as_posix())
# print('\n'.join([s.as_posix(), sys.path))

log = logging.getLogger(__name__)

HOST_DICT = {
    'dev': '127.0.0.1:5000',
    'prod': 'https://dex.edirepository.org',
}


def flask_main(_ctx):
    parser = argparse.ArgumentParser(
        __doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Debug level logging',
    )
    parser.add_argument(
        '--yes-i-am-sure',
        action='store_true',
        help="Required argument acting as speed bump",
    )
    args = parser.parse_args()

    logging.basicConfig(
        format='%(name)s %(levelname)-8s %(message)s',
        level=logging.DEBUG if args.debug else logging.INFO,
        stream=sys.stderr,
    )
    logging.getLogger('matplotlib').setLevel(logging.ERROR)

    if not args.yes_i_am_sure:
        log.error('Missing argument')
        return 1

    dex.util.wipe_cache()

    log.info('Success!')

    return 0


if __name__ == '__main__':
    app = flask.Flask(__name__)
    app.config.from_object("dex.config")
    with app.app_context() as ctx:
        sys.exit(flask_main(ctx))

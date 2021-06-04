#!/usr/bin/env python

"""Wrap pandas_profiling"""

import logging
import pathlib
import pprint
import sys

import flask
import flask.json
import pandas_profiling

import dex.csv_parser

log = logging.getLogger(__name__)


def main():
    app = flask.Flask(__name__)
    app.config.from_object("config")
    with app.app_context():
        return flask_main(app)


def flask_main(app):
    is_debug = '--debug' in sys.argv
    if is_debug:
        sys.argv.remove('--debug')

    logging.basicConfig(
        format='%(name)s %(levelname)-8s %(message)s',
        level=logging.DEBUG if is_debug else logging.INFO,
        stream=sys.stderr,
    )

    logging.getLogger('matplotlib').setLevel(logging.ERROR)

    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} [path to JSON config file]")
        sys.exit(1)

    json_config_path = pathlib.Path(sys.argv[1])
    json_str = json_config_path.read_text()

    arg = flask.json.loads(json_str)
    # pprint.pp(arg, stream=sys.stderr, indent=2, width=1000)

    report_tree = get_report_tree(arg)
    rearrange_report(report_tree)
    html_str = report_tree.to_html()

    # Return the profile report HTML doc to caller via stdout
    print(html_str)

    return 0


def rearrange_report(report_tree):
    # Move the Sample section from the end to the front of the report.
    try:
        section_list = report_tree.report.content["body"].content["items"]
        section_list.insert(1, section_list.pop(-1))

        section_list[0].content['items'][1].content['name'] = 'Notes'
        section_list[0].content['items'][2].content['name'] = 'Reproducibility'
        section_list[0].content['items'][2].content['items'][0].content[
            'name'
        ] = 'Reproducibility'
    except Exception:
        log.exception('Unable to reorder the sections of the Pandas Profiling report')


def get_report_tree(arg_dict):
    ctx = dex.csv_parser.get_parsed_csv_with_context(arg_dict['rid'])
    csv_df = ctx['csv_df']
    # Create a tree representation of the report.
    report_tree = pandas_profiling.ProfileReport(
        csv_df,
        config_file=arg_dict['yml_config_path'],
        dark_mode=arg_dict['dark_mode'],
    )
    return report_tree


if __name__ == '__main__':
    sys.exit(main())

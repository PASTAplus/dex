#!/usr/bin/env python

""""""
import csv
import json
import logging
import pathlib
import pprint
import sys
import types

import pandas as pd

import pandas_profiling

log = logging.getLogger(__name__)


def main():
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
    j = json.loads(json_str, object_hook=lambda d: types.SimpleNamespace(**d))

    log.info('JSON config:')
    (list(map(log.info, pprint.pformat(vars(j)).splitlines())))

    log.debug(f'Reading csv to df. csv={j.csv_path}')

    dialect = csv.excel
    dialect.delimiter = j.csv_dialect.delimiter
    dialect.quotechar = j.csv_dialect.quotechar
    dialect.escapechar = j.csv_dialect.escapechar
    df = pd.read_csv(
        j.csv_path,
        skiprows=j.skip_rows,
        dialect=dialect,
    )

    # log.debug('dtypes before:')
    # log.debug(df.dtypes)

    # \"i\": 0, \"name\": \"Watershed\", \"type\": \"S_TYPE_UNSUPPORTED\"}

    # type_dict = {
    #     'i': i,
    #     'attribute_name': attribute_name,
    #     'type_str': 'S_TYPE_UNSUPPORTED',
    #     'storage_type': storage_type,
    #     'iso_date_format_str': iso_date_format_str,
    #     'c_date_format_str': None,
    #     'number_type': number_type,
    #     'numeric_domain': numeric_domain,
    #     'ratio': ratio,
    # }

    for i, type_dict in enumerate(j.profiling_types):
        print('type_dict', file=sys.stderr)
        pprint.pprint(type_dict, stream=sys.stderr)
        d = type_dict.i
        s = df.iloc[:, d]

        if type_dict.type_str == 'TYPE_DATE':
            log.info(f'Column "{type_dict.attribute_name}" -> {type_dict.type_str}')
            s = pd.to_datetime(s, errors='ignore', format=type_dict.c_date_format_str)

        elif type_dict.type_str == 'TYPE_NUM':
            log.info(f'Column "{type_dict.attribute_name}" -> {type_dict.type_str}')
            s = pd.to_numeric(
                s,
                errors='ignore',
            )

        elif type_dict.type_str == 'TYPE_CAT':
            log.info(f'Column "{type_dict.attribute_name}" -> {type_dict.type_str}')
            s = s.astype('category', errors='ignore')

        else:
            pass

        df.iloc[:, d] = s

        # if dt is not None:
        #     df.iloc[:, type_dict['i']] = df.iloc[:, type_dict['i']].to_numpy(dtype=dt)
        # dtype=pd.to_datetime()
        # df.iloc[:, 0] = df.iloc[:, 0].astype(dtype=pd.StringDtype())

    # Create a tree representation of the report.
    report_tree = pandas_profiling.ProfileReport(
        df,
        config_file=j.yml_config_path,
        dark_mode=j.dark_mode,
        # dtype_list=type_list
    )

    # Move the Sample section from the end to the front of the report.
    try:
        section_list = report_tree.report.content["body"].content["items"]
        section_list.insert(1, section_list.pop(-1))

        section_list[0].content['items'][1].content['name'] = 'Notes'
        section_list[0].content['items'][2].content['name'] = 'Reproducibility'
        section_list[0].content['items'][2].content['items'][0].content[
            'name'
        ] = 'Reproducibility'
    except Exception as e:
        log.warning(
            f'Unable to reorder the sections of the Pandas Profiling report. '
            f'Error: {repr(e)}'
        )

    html_str = report_tree.to_html()

    # Return the profile report HTML doc to caller via stdout
    print(html_str)

    return 0


if __name__ == '__main__':
    sys.exit(main())

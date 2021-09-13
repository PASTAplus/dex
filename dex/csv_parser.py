"""Handle initial parsing of .csv files into Pandas DataFrames.

This parses the .csv files according to the type declarations for each column in the
corresponding EML documents.
"""
import contextlib
import csv
import datetime
import functools
import logging
import time

import clevercsv
import clevercsv.cparser
import clevercsv.dialect
import numpy as np
import pandas as pd
from flask import current_app as app

import dex.db
# import dex.cache
# import dex.csv_cache
import dex.eml_cache
import dex.eml_types
import dex.exc
import dex.obj_bytes
import dex.pasta
import dex.util

log = logging.getLogger(__name__)

DTYPE_TO_FRIENDLY_DICT = {
    'S_TYPE_UNSUPPORTED': 'Generic',
    'TYPE_CAT': 'Categorical',
    'TYPE_DATE': "Time series",
    'TYPE_INT': "Numeric",
    'TYPE_NUM': "Numeric",
}


# @dex.cache.disk("parsed-csv-context", "df")
def get_parsed_csv_with_context(rid):
    """Get CSV with values parsed according to their EML types."""
    ctx = get_csv_context(rid)
    log.debug(f'ctx[\'header_list\']={ctx["header_list"]}')
    log.debug(f'ctx[\'parser_dict\']={ctx["parser_dict"]}')
    parser_func_dict = {ctx['header_list'][k]: d['fn'] for k, d in ctx['parser_dict'].items()}
    pandas_type_dict = {
        ctx['header_list'][k]: d['pandas_type'] for k, d in ctx['parser_dict'].items()
    }
    missing_code_set = set()
    for type_dict in ctx['derived_dtypes_list']:
        if isinstance(type_dict['missing_code_list'], list):
            for code_str in type_dict['missing_code_list']:
                missing_code_set.add(code_str)

    missing_code_set.add('')

    csv_df = get_parsed_csv(
        rid,
        header_row_idx=ctx['header_row_idx'],
        parser_dict=parser_func_dict,  # ctx['parser_dict'],
        dialect=ctx['dialect'],
        pandas_type_dict=pandas_type_dict,
        na_list=list(missing_code_set),
    )

    return dict(
        csv_df=csv_df,
        csv_path=ctx['csv_path'],
        derived_dtypes_list=ctx['derived_dtypes_list'],
        header_list=ctx['header_list'],
        header_row_idx=ctx['header_row_idx'],
        parser_dict=ctx['parser_dict'],
        col_agg_dict=get_col_agg_dict(csv_df, ctx['derived_dtypes_list']),
        dialect=ctx['dialect'],
    )


# @dex.cache.disk("csv-context", "pickle")
def get_csv_context(rid):
    """Get the information that is required for parsing the CSV."""
    derived_dtypes_list = get_derived_dtypes_from_eml(rid)
    parser_dict = get_parser_dict(derived_dtypes_list)
    csv_stream = dex.obj_bytes.open_csv(rid)
    dialect = get_dialect(csv_stream)
    header_row_idx, header_list = find_header_row(csv_stream)
    ctx = dict(
        dialect=dialect,
        csv_path=csv_stream,
        derived_dtypes_list=derived_dtypes_list,
        header_list=header_list,
        header_row_idx=header_row_idx,
        parser_dict=parser_dict,
    )
    dex.util.logpp(ctx, msg='Parsed CSV context', logger=log.debug)
    return ctx


def get_derived_dtype_list(rid):
    entity_tup = dex.db.get_entity(rid)
    pkg_id = dex.pasta.get_pkg_id_as_url(entity_tup)
    # eml_url = dex.pasta.get_eml_url(entity_tup)
    # eml_path = dex.pasta.get_eml_path(entity_tup)
    # return _get_path(eml_path, eml_url)
    eml_el = dex.eml_cache.get_eml_etree(rid)
    # data_url = dex.obj_bytes['get_data_path_by_row_id'](rid)
    dt_el = dex.eml_types.get_data_table_by_package_id(eml_el, pkg_id)
    derived_dtype_list = dex.eml_types.get_derived_dtypes_from_eml(dt_el)
    return derived_dtype_list


def cast_to_eml_types(df, rid):
    """In-place conversion of columns from their Pandas types as detected by Pandas to
    the EML derived_dtype types.
    """
    derived_dtype_list = get_derived_dtype_list(rid)

    for col_idx, dtype_dict in enumerate(derived_dtype_list):
        assert col_idx == col_idx
        d = dict(**dtype_dict)
        s = df.iloc[:, d['col_idx']]

        if d['type_str'] == 'TYPE_DATE':
            log.debug(f'Column "{d["col_name"]}" -> {d["type_str"]}')
            s = pd.to_datetime(s, errors='ignore', format=d['c_date_fmt_str'])

        elif d['type_str'] == 'TYPE_NUM':
            log.debug(f'Column "{d["col_name"]}" -> {d["type_str"]}')
            s = pd.to_numeric(s, errors='ignore')

        elif d['type_str'] == 'TYPE_CAT':
            log.debug(f'Column "{d["col_name"]}" -> {d["type_str"]}')
            s = s.astype('category', errors='ignore')

        else:
            pass

        df.iloc[:, d['col_idx']] = s


def get_dialect(csv_path, verbose=False):
    """Get the dialect (type of delimiter, quotes and escape characters) for the CSV.
    The dialect is passed to pandas.read_csv() to improve parsing.
    """
    clever_dialect = get_clevercsv_dialect(csv_path, verbose)
    return _get_native_dialect(clever_dialect)


def get_clevercsv_dialect(csv_path, verbose=False):
    # log.debug("clevercsv - start dialect discovery")
    with open(csv_path, "r", newline="") as fp:
        clever_dialect = clevercsv.Sniffer().sniff(
            fp.read(app.config["CSV_SNIFFER_THRESHOLD"]),
            verbose=verbose,
        )
    if clever_dialect.quotechar == '':
        clever_dialect.quotechar = '"'

    log.debug(f'clevercsv_dialect="{clever_dialect}"')

    if clever_dialect == clevercsv.dialect.SimpleDialect(',', '', ''):
        raise dex.exc.CSVError(f'Unable to find header row in CSV. csv_path="{csv_path}"')

    return clever_dialect


def _get_native_dialect(clever_dialect):
    """Translate from clevercsv.SimpleDialect() to csv.Dialect()"""
    dialect = csv.excel
    c = clever_dialect
    dialect.delimiter = c.delimiter
    dialect.quotechar = c.quotechar if c.quotechar else '"'
    dialect.escapechar = c.escapechar if c.escapechar else None
    return dialect


def get_dialect_as_dict(native_dialect):
    """Can be used as kwargs for creating a new dialect object"""
    n = native_dialect
    return dict(
        delimiter=n.delimiter,
        doublequote=n.doublequote,
        escapechar=n.escapechar,
        lineterminator=n.lineterminator,
        quotechar=n.quotechar,
        quoting=n.quoting,
        skipinitialspace=n.skipinitialspace,
    )


def find_header_row(csv_path):
    """Return a 2-tuple with the index of the first row that looks like the headers in a
    CSV file, and a list of the header names.

    This returns the first row that:

        - Has the same number of columns as most of the other rows in the CSV file
        - Does not have pure numbers in any of the fields
        - Does not have any empty fields

    If none of the rows that are checked fill the criteria, returns 0, the index of
    the top row.
    """
    clevercsv_dialect = get_clevercsv_dialect(csv_path)
    count_dict = {}
    with open(csv_path, "r", newline="") as f:
        for row_idx, row in enumerate(clevercsv.reader(f, clevercsv_dialect)):
            if row_idx == app.config["CSV_SNIFFER_THRESHOLD"]:
                break
            count_dict.setdefault(len(row), 0)
            count_dict[len(row)] += 1

    max_row_count = max(count_dict.values()) if count_dict else 0
    for col_count, row_count in count_dict.items():
        if row_count == max_row_count:
            column_count = col_count
            break
    else:
        # assert False
        raise dex.exc.RedirectToIndex()

    log.debug(f"column_count: {column_count}")

    with open(csv_path, "r", newline="") as f:
        for row_idx, row in enumerate(clevercsv.reader(f, clevercsv_dialect)):
            if row_idx == 100:
                break
            # log.debug(f"checking row: {row}")
            if len(row) != column_count:
                continue
            if any(s.isnumeric() for s in row):
                continue
            if any(not s for s in row):
                continue
            return row_idx, row

    raise dex.exc.CSVError(f'Unable to find header row in CSV. csv_path="{csv_path}"')


def apply_parsers(df, derived_dtypes_list):
    parser_list = get_parser_list(derived_dtypes_list)
    for i, (parser_dict, column_name) in enumerate(zip(parser_list, df.columns)):
        df.iloc[:, i].map(parser_dict['fn'])


def get_col_agg_dict(df, derived_dtypes_list):
    """Calculate per column aggregates."""
    formatter_list = get_formatter_list(derived_dtypes_list)
    d = {}
    for i, col_name in enumerate(df.columns):
        fmt_fn = formatter_list[i]['fn']
        with contextlib.suppress(Exception):
            d[i] = dict(
                col_name=col_name,
                v_max=fmt_fn(df.iloc[:, i].max(skipna=True)),
                v_min=fmt_fn(df.iloc[:, i].min(skipna=True)),
            )
    return d


def apply_formatters(df, derived_dtypes_list):
    formatter_list = get_formatter_list(derived_dtypes_list)
    for i in range(len(derived_dtypes_list)):
        df.iloc[:, i] = df.iloc[:, i].map(formatter_list[i]['fn'])
    return df


def get_parser_dict(derived_dtypes_list):
    return {eml_dict['col_idx']: get_parser(eml_dict) for eml_dict in derived_dtypes_list}


def get_formatter_dict(derived_dtypes_list):
    # util.logpp(derived_dtypes_list)
    return {eml_dict['col_idx']: get_formatter(eml_dict) for eml_dict in derived_dtypes_list}


def get_parser_list(derived_dtypes_list):
    return list([get_parser(t) for t in derived_dtypes_list])


def get_formatter_list(derived_dtypes_list):
    return list([get_formatter(t) for t in derived_dtypes_list])


def get_formatter(dtype_dict):
    def date_formatter(x, fmt_str):
        try:
            return datetime.datetime.strftime(x, fmt_str)
        except Exception:
            return str(x)

    def string_formatter(x):
        return str(x)

    def float_formatter(x):
        return f'{x:.02f}'

    def int_formatter(x):
        return str(x)

    # if isinstance(dtype_dict, dict):
    #     d = dict(**dtype_dict)
    # else:
    #     d = dtype_dict

    d = dict(**dtype_dict)

    if d['type_str'] == 'S_TYPE_UNSUPPORTED':
        return dict(
            name='string pass',
            fmt=None,
            dtype=d['type_str'],
            fn=string_formatter,
            pandas_type='object',
        )

    elif d['type_str'] == 'TYPE_DATE':
        return dict(
            name=f'format datetime to "{d["c_date_fmt_str"]}"',
            fmt=None,
            dtype=d['type_str'],
            fn=functools.partial(date_formatter, fmt_str=d['c_date_fmt_str']),
            pandas_type='datetime64',
        )

    elif d['type_str'] == 'TYPE_INT' or d['type_str'] == 'TYPE_NUM':
        if d['date_fmt_str'] is not None:
            return dict(
                name='format integer',
                fmt=None,
                dtype=None,
                fn=int_formatter,
                pandas_type='int64',
            )
        elif d['number_type'] in ('real', 'integer', 'whole', 'natural', 'integer'):
            return dict(
                name='format integer',
                fmt=d['number_type'],
                dtype=None,
                fn=float_formatter,
                pandas_type='int64',
            )
        elif d['number_type'] in ('float', 'floating-point'):
            return dict(
                name='format floating point',
                fmt=d['number_type'],
                dtype=None,
                fn=float_formatter,
                pandas_type='float64',
            )
        else:
            return dict(
                name='unknown pass',
                fmt=None,
                dtype=None,
                fn=string_formatter,
                pandas_type='object',
            )

    elif d['type_str'] == 'TYPE_CAT':
        return dict(
            name='categorical pass',
            fmt=d['number_type'],
            dtype=None,
            fn=string_formatter,
            pandas_type='category',
        )

    else:
        return dict(
            name='unknown pass',
            fmt=None,
            dtype=None,
            fn=string_formatter,
            pandas_type='object',
        )


def get_parser(dtype_dict):
    # TODO: Move parsers to this pattern
    def parse_date_optimized(date_series_in, date_fmt_str=None):
        """Date parser optimized for the common case where the same date appears in
        many rows. The formatter runs only once for each unique date, and the result
        is applied to all dates in the set.
        """
        date_series = {
            unique_date_ser: pd.to_datetime(unique_date_ser, format=date_fmt_str)
            for unique_date_ser in date_series_in.unique()
        }
        return date_series_in.map(date_series)

    def date_parser(x, fmt_str):
        try:
            return datetime.datetime.strptime(x, fmt_str)
        except Exception:
            return str(x)

    def string_parser(x):
        return str(x)

    def float_parser(x):
        # return lambda x: f'{_parse_int(x):0{len(d['date_fmt_str'])}}'
        try:
            return float(x)
        except ValueError:
            return 0.0

    def int_parser(x):
        # return lambda x: f'{_parse_int(x)}'
        try:
            return int(x)
        except ValueError:
            return 0

    # if isinstance(dtype_dict, dict):
    #     d = dict(**dtype_dict)
    # else:
    #     d = dtype_dict

    d = dict(**dtype_dict)

    if d['type_str'] == 'S_TYPE_UNSUPPORTED':
        return dict(
            name='string pass',
            fmt=None,
            dtype=d['type_str'],
            fn=string_parser,
            pandas_type='object',
        )

    elif d['type_str'] == 'TYPE_DATE':
        return dict(
            name=f'parse datetime',
            fmt=d["c_date_fmt_str"],
            dtype=d['type_str'],
            fn=functools.partial(date_parser, fmt_str=d['c_date_fmt_str']),
            pandas_type='datetime64',
        )

    elif d['type_str'] == 'TYPE_INT' or d['type_str'] == 'TYPE_NUM':
        if d['date_fmt_str'] is not None:
            return dict(
                name='parse integer',
                fmt='integer',
                dtype=d['type_str'],
                fn=int_parser,
                pandas_type='int64',
            )
        elif d['number_type'] in ('real', 'integer', 'whole', 'natural', 'integer'):
            return dict(
                name='parse integer',
                fmt=d['number_type'],
                dtype=d['type_str'],
                fn=int_parser,
                pandas_type='int64',
            )
        elif d['number_type'] in ('float', 'floating-point'):
            return dict(
                name='parse floating point',
                fmt=d['number_type'],
                dtype=d['type_str'],
                fn=float_parser,
                pandas_type='float64',
            )
        else:
            return dict(
                name='unknown pass',
                fmt=None,
                dtype=d['type_str'],
                fn=string_parser,
                pandas_type='object',
            )

    elif d['type_str'] == 'TYPE_CAT':
        return dict(
            name='categorical pass',
            fmt=None,
            dtype=d['type_str'],
            fn=string_parser,
            pandas_type='category',
        )
    else:
        return dict(
            name='unknown pass',
            fmt=None,
            dtype=d['type_str'],
            fn=string_parser,
            pandas_type='object',
        )


def get_derived_dtypes_from_eml(rid):
    dt_el = dex.eml_cache.get_data_table(rid)
    return dex.eml_types.get_derived_dtypes_from_eml(dt_el)


# @dex.cache.disk("raw-csv", "pickle")
def get_raw_csv_with_context(rid, max_rows=1000):
    """Get CSV with minimal processing"""
    ctx = get_raw_context(rid)
    # ctx['top_list']
    csv_df = get_raw_csv(
        csv_path=ctx['csv_path'],
        dialect=ctx['dialect'],
        header_row_idx=ctx['header_row_idx'],
        max_rows=max_rows,
    )
    return dict(
        csv_df=csv_df,
        csv_path=ctx['csv_path'],
        header_row_idx=ctx['header_row_idx'],
        header_list=ctx['header_list'],
        raw_line_count=len(csv_df),
    )


def get_raw_context(rid):
    csv_stream = dex.obj_bytes.open_csv(rid)
    dialect = get_dialect(csv_stream)
    header_row_idx, header_list = find_header_row(csv_stream)
    return dict(
        dialect=dialect,
        csv_path=csv_stream,
        header_row_idx=header_row_idx,
        header_list=header_list,
    )


# @dex.cache.disk("parsed", "df")
def get_parsed_csv(rid, header_row_idx, parser_dict, dialect, pandas_type_dict, na_list):
    """Read a CSV and parse each value (cell) to the type declared for its column in the
    EML.

    - Pandas supports a basic set of types, while EML supports much more complex type
    declarations and descriptions. The columns that are parsed by this method are the
    ones for which we are currently able to derive a Pandas type based on the EML. The
    remaining columns are handles by Pandas.

    - This is split out to a separate function so that the CSV can be cached separately.

    - Empty field ("") is unconditionally added as a NaN value here, as it's not always
    declared in the EML.
    """
    csv_stream = dex.obj_bytes.open_csv(rid)
    csv_df = pd.read_csv(
        filepath_or_buffer=csv_stream,
        index_col=False,
        skiprows=header_row_idx,
        na_filter=True,
        na_values=list(set(na_list) | {''}),
        keep_default_na=True,
        # skip_blank_lines=False,
        # nrows=col_idx,
        converters=parser_dict,
        #
        dialect=dialect,
        header=0,
        # names=[0]*100,
        # cache_dates, which defaults to True, speeds up datetime parsing for cases
        # having many repeated dates. This goes into effect only if the column is not
        # parsed by Dex.
        cache_dates=True,
    )

    # csv_df['PET'].replace(to_replace=[''], value=np.nan, inplace=True)
    # csv_df.replace(value=np.nan, regex='^\s*\$', inplace=True)

    return csv_df.astype(pandas_type_dict, errors='ignore')


def get_raw_csv(csv_path, dialect, header_row_idx, max_rows):
    csv_df = pd.read_csv(
        # index_col=False,
        header=0,
        index_col=False,
        filepath_or_buffer=csv_path,
        skiprows=header_row_idx,
        na_filter=False,
        skip_blank_lines=False,
        nrows=max_rows,
        dialect=dialect,
        # Setting dtype to str and providing no valid matches for NaNs, disables the
        # automatic parsing in Pandas. and gives us the unprocessed text values of the
        # fields.
        dtype=str,
        na_values=[],
    )
    return csv_df

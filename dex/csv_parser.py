"""Handle initial parsing of .csv file into Pandas DataFrame.

This parses the .csv files according to the type declarations for each column in the
corresponding EML documents.
"""
import contextlib
import datetime
import functools
import logging

import numpy as np
import pandas as pd

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
    ctx = get_eml_ctx(rid)
    dex.util.logpp(ctx, msg='EML CTX', logger=log.debug)

    parser_func_dict = {ctx['col_name_list'][k]: d['fn'] for k, d in ctx['parser_dict'].items()}
    dex.util.logpp(ctx, msg='parser_func_dict', logger=log.debug)

    pandas_type_dict = {
        ctx['col_name_list'][k]: d['pandas_type'] for k, d in ctx['parser_dict'].items()
    }
    missing_code_set = set()
    for type_dict in ctx['derived_dtypes_list']:
        if isinstance(type_dict['missing_code_list'], list):
            for code_str in type_dict['missing_code_list']:
                missing_code_set.add(code_str)

    missing_code_set.add('')

    csv_df = get_parsed_csv(
        rid,
        header_line_count=ctx['header_line_count'],
        footer_line_count=ctx['footer_line_count'],
        parser_dict=parser_func_dict,
        dialect=ctx['dialect'],
        pandas_type_dict=pandas_type_dict,
        na_list=list(missing_code_set),
    )

    csv_df = apply_nan(csv_df, missing_code_set)

    return dict(
        csv_df=csv_df,
        derived_dtypes_list=ctx['derived_dtypes_list'],
        col_name_list=ctx['col_name_list'],
        header_line_count=ctx['header_line_count'],
        parser_dict=ctx['parser_dict'],
        col_agg_dict=get_col_agg_dict(csv_df, ctx['derived_dtypes_list']),
        dialect=ctx['dialect'],
    )


# @dex.cache.disk("eml-ctx", "pickle")
def get_eml_ctx(rid):
    """Get the EML information that is required for parsing the CSV."""
    dt_el = dex.eml_cache.get_data_table(rid)
    derived_dtypes_list = dex.eml_types.get_derived_dtypes_from_eml(dt_el)
    return dict(
        derived_dtypes_list=derived_dtypes_list,
        dialect=dex.eml_types.get_dialect(dt_el),
        header_line_count=dex.eml_types.get_header_line_count(dt_el),
        footer_line_count=dex.eml_types.get_footer_line_count(dt_el),
        parser_dict=get_parser_dict(derived_dtypes_list),
        col_name_list=[d['col_name'] for d in derived_dtypes_list],
    )


# def get_csv_stream(rid):


def _get_derived_dtype_list(rid):
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


def get_dialect_as_dict(dialect):
    """Can be used as kwargs for creating a new dialect object"""
    return dialect.__dict__


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
        #     s = pd.to_datetime(s, errors='ignore', format=d['c_date_fmt_str'])
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
            name='string passthrough',
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

    elif d['type_str'] in ('TYPE_INT', 'TYPE_NUM'):

        if d['number_type'] in ('real', 'integer', 'whole', 'natural', 'integer'):
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
                name='unknown passthrough',
                fmt=None,
                dtype=d['type_str'],
                fn=string_parser,
                pandas_type='object',
            )

    elif d['type_str'] == 'TYPE_CAT':
        return dict(
            name='categorical passthrough',
            fmt=None,
            dtype=d['type_str'],
            fn=string_parser,
            pandas_type='category',
        )
    else:
        return dict(
            name='unknown passthrough',
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
        csv_stream=ctx['csv_stream'],
        dialect=ctx['dialect'],
        header_line_count=ctx['header_line_count'],
        max_rows=max_rows,
    )
    return dict(
        csv_df=csv_df,
        csv_stream=ctx['csv_stream'],
        header_line_count=ctx['header_line_count'],
        col_name_list=ctx['col_name_list'],
        raw_line_count=len(csv_df),
    )


# @dex.cache.disk("parsed", "df")
def get_parsed_csv(
    rid,
    header_line_count,
    footer_line_count,
    parser_dict,
    dialect,
    pandas_type_dict,
    na_list,
    max_rows=None,
):
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

    dex.util.logpp(parser_dict, msg='parser_dict', logger=log.error)


    csv_df = pd.read_csv(
        # Commented lines show the defaults
        filepath_or_buffer=csv_stream,
        header=0,
        # header=None,  # Do not use column names from the CSV (we get them from the EML)
        names=pandas_type_dict.keys(),  # Use column names from EML

        index_col=False,  # Do not use the first column as the index

        # Only get the number of columns that are declared in the EML.
        # Thi resolves issues where lines have trailing empty columns.
        usecols=range(len(pandas_type_dict)),

        # squeeze=False,
        # prefix=NoDefault.no_default,
        # mangle_dupe_cols=True,
        # engine=None,
        converters=parser_dict,
        # true_values=None,
        # false_values=None,
        skiprows=header_line_count,
        skipfooter=footer_line_count,  # Not required when setting nrows
        nrows=max_rows,  # Read the number of rows declared in the EML
        na_filter=True,
        na_values=list(set(na_list)),
        # Add common NaNs:
        # ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’,
        # ‘<NA>’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’
        keep_default_na=True,
        dialect=dialect,  # Use dialect from EML, not inferred
        # delimiter=None, # Alias for 'sep'. Overridden by setting dialect
        # doublequote=True, # Overridden by setting dialect
        # escapechar=None, # Overridden by setting dialect
        # quotechar='"', # Overridden by setting dialect
        # quoting=0, # Overridden by setting dialect
        # sep=NoDefault.no_default, # Alias for 'delimiter'. Overridden by setting dialect
        # skipinitialspace=False, # Overridden by setting dialect
        skip_blank_lines=False,
        # verbose=False,
        # parse_dates=False,
        # infer_datetime_format=False,
        # keep_date_col=False,
        # date_parser=None,
        # dayfirst=False,
        # Speeds up datetime parsing for cases having many repeated dates. This goes
        # into effect only if the column is not parsed by Dex.
        # cache_dates=True,
        # iterator=False,
        # chunksize=None,
        # compression='infer',
        # thousands=None,
        # decimal='.',
        # lineterminator=None,
        # comment=None,
        # encoding=None,
        # encoding_errors='strict',
        # error_bad_lines=None,
        # warn_bad_lines=None,
        # on_bad_lines=None,
        # delim_whitespace=False,
        # low_memory=True,
        # memory_map=False,
        # float_precision=None,
        # storage_options=None,
    )

    # print(csv_df.describe())
    csv_df.info()

    return csv_df

    # csv_df['PET'].replace(to_replace=[''], value=np.nan, inplace=True)
    # csv_df.replace(value=np.nan, regex='^\s*\$', inplace=True)

    # return csv_df.astype(pandas_type_dict, errors='ignore')


def get_raw_csv(csv_stream, dialect, header_line_count, max_rows):
    """Read CSV to DF with minimal processing

    The purpose of this function is to provide a view of the source CSV data.
    """
    csv_df = pd.read_csv(
        filepath_or_buffer=csv_stream,
        index_col=False,
        skiprows=header_line_count,
        na_filter=False,
        header=0,
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


def apply_nan(df, nan_set: set):
    """Set all values in the df that equal a value in the nan_set to Nan (numpy.nan)"""
    # Add string versions of the various NaN values.
    nan_set.update([str(x) for x in nan_set])
    return df.applymap(lambda x: np.nan if x in nan_set else x)


# def is_csv(_rid, csv_stream):
#     try:
#         clevercsv_dialect = dex.csv_parser.get_clevercsv_dialect(csv_stream)
#         count_dict = {}
#         with open(csv_stream, "r", newline="", encoding='utf-8') as f:
#             for i, row in enumerate(clevercsv.reader(f, clevercsv_dialect)):
#                 if i == 100:
#                     break
#                 count_dict.setdefault(len(row), 0)
#                 count_dict[len(row)] += 1
#         return 1 <= len(count_dict) <= 3
#     except Exception:
#         return False


# def _load_csv_to_df_slow(csv_stream):
#     """This method handles various broken CSV files but is too slow for use in
#     production.
#     """
#     skip_rows, header_row = dex.csv_parser.find_header_row(csv_stream)
#     log.debug(f"skip_rows: {skip_rows}")
#     log.debug("clevercsv - read_dataframe start")
#     start_ts = time.time()
#     csv_df = clevercsv.read_dataframe(csv_stream, skiprows=skip_rows)
#     log.debug(f"clevercsv - read_dataframe: {time.time() - start_ts:.02f}s")
#     return csv_df

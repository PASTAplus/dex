"""Handle initial parsing of .csv file into Pandas DataFrame.

This parses the .csv files according to the type declarations for each column in the
corresponding EML documents.
"""
import datetime
import functools
import logging

import numpy as np
import pandas as pd
from flask import current_app as app

import dex.db
import dex.eml_cache
import dex.eml_types
import dex.exc
import dex.obj_bytes
import dex.pasta
import dex.util

log = logging.getLogger(__name__)


# We cache the returned objects individually here.
def get_parsed_csv_with_context(rid):
    """Get CSV with values parsed according to their EML types."""
    eml_ctx = get_eml_ctx(rid)
    csv_df = get_parsed_csv(rid, eml_ctx)
    raw_df = get_raw_csv(rid, eml_ctx)
    return csv_df, raw_df, eml_ctx


# @dex.cache.disk("eml-ctx", "pickle")
def get_eml_ctx(rid):
    """Get the EML information that is required for parsing the CSV."""
    dt_el = dex.eml_cache.get_data_table(rid)
    column_list = dex.eml_types.get_col_attr_list(dt_el)
    parser_func_dict = get_parser_dict(column_list)
    col_name_list = [d['col_name'] for d in column_list]
    pandas_type_dict = {d['col_name']: d['pandas_type'] for d in column_list}

    missing_code_set = set()

    for type_dict in column_list:
        if isinstance(type_dict['missing_code_list'], list):
            for code_str in type_dict['missing_code_list']:
                missing_code_set.add(code_str)

    ctx = dict(
        column_list=column_list,
        dialect=dex.eml_types.get_dialect(dt_el),
        header_line_count=dex.eml_types.get_header_line_count(dt_el),
        footer_line_count=dex.eml_types.get_footer_line_count(dt_el),
        parser_dict=parser_func_dict,
        col_name_list=col_name_list,
        parser_func_dict=parser_func_dict,
        pandas_type_dict=pandas_type_dict,
        missing_code_set=missing_code_set,
        missing_code_list=list(missing_code_set),
    )

    dex.util.logpp(ctx, msg='EML CTX', logger=log.debug)

    return ctx


def _get_column_list(rid):
    entity_tup = dex.db.get_entity(rid)
    pkg_id = dex.pasta.get_pkg_id_as_url(entity_tup)
    eml_el = dex.eml_cache.get_eml_etree(rid)
    dt_el = dex.eml_types.get_data_table_by_package_id(eml_el, pkg_id)
    column_list = dex.eml_types.get_col_attr_list(dt_el)
    return column_list


def get_dialect_as_dict(dialect):
    """Can be used as kwargs for creating a new dialect object"""
    return dialect.__dict__


def get_parser_dict(column_list):
    return {eml_dict['col_idx']: get_parser(eml_dict) for eml_dict in column_list}


def get_parser_list(column_list):
    return list([get_parser(t) for t in column_list])


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
            return None

    def string_parser(x):
        return str(x)

    def float_parser(x):
        try:
            return float(x)
        except ValueError:
            return None

    def int_parser(x):
        try:
            return int(x)
        except ValueError:
            return pd.NA

    d = N(**dtype_dict)

    if d.pandas_type == dex.eml_types.PandasType.FLOAT:
        return float_parser
    elif d.pandas_type == dex.eml_types.PandasType.INT:
        return int_parser
    elif d.pandas_type == dex.eml_types.PandasType.CATEGORY:
        return string_parser
    elif d.pandas_type == dex.eml_types.PandasType.DATETIME:
        return functools.partial(date_parser, fmt_str=d.c_date_fmt_str)
    elif d.pandas_type == dex.eml_types.PandasType.STRING:
        return string_parser
    else:
        raise AssertionError(f'Invalid PandasType: {d.pandas_type}')


def get_derived_dtypes_from_eml(rid):
    dt_el = dex.eml_cache.get_data_table(rid)
    return dex.eml_types.get_col_attr_list(dt_el)


# @dex.cache.disk("parsed-csv", "df")
def get_parsed_csv(rid, eml_ctx):
    return _get_csv(rid, eml_ctx, do_parse=True)


# @dex.cache.disk("raw-csv", "df")
def get_raw_csv(rid, eml_ctx):
    return _get_csv(rid, eml_ctx, do_parse=False)


def _get_csv(rid, eml_ctx, do_parse):
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

    # Commented lines show the defaults
    arg_dict = dict(
        filepath_or_buffer=csv_stream,
        header=None,  # Do not use column names from the CSV (we get them from the EML)
        names=eml_ctx['pandas_type_dict'].keys(),  # Use column names from EML
        index_col=False,  # Do not use the first column as the index
        # Only get the number of columns that are declared in the EML.
        # Thi resolves issues where lines have trailing empty columns.
        usecols=range(len(eml_ctx['pandas_type_dict'])),
        # squeeze=False,
        # prefix=NoDefault.no_default,
        # mangle_dupe_cols=True,
        # TODO, check if we can use the C version
        engine='python',
        skiprows=eml_ctx['header_line_count'],
        skipfooter=eml_ctx['footer_line_count'],
        nrows=app.config['CSV_MAX_CELLS'] // len(eml_ctx['column_list']),
        dialect=eml_ctx['dialect'],  # Use dialect from EML, not inferred
        # We cannot skip blank lines here, as we need the number of rows of the parsed
        # CSV to always match that of the raw CSV.
        skip_blank_lines=False,
        # delimiter=None, # Alias for 'sep'. Overridden by setting dialect
        # doublequote=True, # Overridden by setting dialect
        # escapechar=None, # Overridden by setting dialect
        # quotechar='"', # Overridden by setting dialect
        # quoting=0, # Overridden by setting dialect
        # sep=NoDefault.no_default, # Alias for 'delimiter'. Overridden by setting dialect
        # skipinitialspace=False, # Overridden by setting dialect
        # verbose=False,
        # parse_dates=False,
        # infer_datetime_format=False,
        # keep_date_col=False,
        # date_parser=None,
        # dayfirst=False,
        # Speeds up datetime parsing for cases having many repeated dates. This goes
        # into effect only if the column is not parsed by DeX.
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

    # Parse the CSV
    if do_parse:
        arg_dict.update(dict(
            converters=eml_ctx['parser_func_dict'],
            # true_values=None,
            # false_values=None,
            # Not required when setting skiprows and skipfooter. Read the number of rows declared in the EML
            # nrows=max_rows,
            na_filter=True,
            na_values=list(set(eml_ctx['missing_code_list'])),
            # na_values=['', None],
            # Add common NaNs:
            # ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’,
            # ‘<NA>’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’
            keep_default_na=True,
        ))

    # Raw CSV
    else:
        arg_dict.update(
            dict(
                # Setting dtype to str and providing no valid matches for NaNs, disables the
                # automatic parsing in Pandas and gives us the unprocessed text values of the
                # fields.
                dtype=str,
                na_filter=False,
                na_values=[],

            )
        )

    try:
        csv_df = pd.read_csv(**arg_dict)
    except ValueError as e:
        raise dex.exc.CSVError(str(e))

    if do_parse:
        # print(csv_df.describe())
        log.debug('#' * 100)
        csv_df.info()
        # log.debug(len(csv_df))
        log.debug('#' * 100)

    # csv_df['PET'].replace(to_replace=[''], value=np.nan, inplace=True)
    # csv_df.replace(value=np.nan, regex='^\s*\$', inplace=True)
    # return csv_df.astype(pandas_type_dict, errors='ignore')

    return csv_df



def apply_nan(df, nan_set: set):
    """Set all values in the df that equal a value in the nan_set to Nan (numpy.nan)"""
    # Add string versions of the various NaN values.
    nan_set.update([str(x) for x in nan_set])
    return df.applymap(lambda x: np.nan if x in nan_set else x)

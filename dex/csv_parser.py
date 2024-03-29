"""Handle initial parsing of .csv file into Pandas DataFrame.

This parses the .csv files according to the type declarations for each column in the
corresponding EML documents.
"""
import logging
import pprint

import numpy as np
import pandas as pd
from flask import current_app as app

import dex.cache
import dex.db
import dex.eml_cache
import dex.eml_extract
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
    """Get the EML information that is required for parsing and processing the CSV."""
    dt_el = dex.eml_cache.get_data_table_el(rid)
    column_list = dex.eml_extract.get_col_attr_list(dt_el)
    ctx = dict(
        column_list=column_list,
        dialect=dex.eml_extract.get_dialect(dt_el),
        header_line_count=dex.eml_extract.get_header_line_count(dt_el),
        footer_line_count=dex.eml_extract.get_footer_line_count(dt_el),
        parser_dict=get_parser_dict(column_list),
        formatter_dict=get_formatter_dict(column_list),
        col_name_list=[d['col_name'] for d in column_list],
        pandas_type_dict={d['col_name']: d['pandas_type'] for d in column_list},
        missing_code_dict={d['col_idx']: d['missing_code_list'] for d in column_list},
    )
    dex.util.logpp(ctx, msg='EML CTX', logger=log.debug)
    return ctx


def get_dialect_as_dict(dialect):
    """Can be used as kwargs for creating a new dialect object"""
    return dialect.__dict__


def get_parser_dict(column_list):
    return {eml_dict['col_idx']: get_parser(eml_dict) for eml_dict in column_list}


def get_formatter_dict(column_list):
    return {eml_dict['col_idx']: get_formatter(eml_dict) for eml_dict in column_list}


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

    def date_parser(x, fmt_fn):
        try:
            return fmt_fn(x)
        except Exception:
            return None

    def string_parser(x):
        return str(x)

    def float_parser(x):
        try:
            return float(x)
        except (ValueError, TypeError):
            return None

    def int_parser(x):
        try:
            return int(x)
        except (ValueError, TypeError):
            # return pd.NA
            return None

    d = N(**dtype_dict)

    if d.pandas_type == dex.eml_extract.PandasType.FLOAT:
        return float_parser
    elif d.pandas_type == dex.eml_extract.PandasType.INT:
        return int_parser
    elif d.pandas_type == dex.eml_extract.PandasType.CATEGORY:
        return string_parser
    elif d.pandas_type == dex.eml_extract.PandasType.DATETIME:
        return d.date_fmt_dict['parser']
    elif d.pandas_type == dex.eml_extract.PandasType.STRING:
        return string_parser
    else:
        raise AssertionError(f'Invalid PandasType: {d.pandas_type}')


def get_formatter(dtype_dict):
    def string_formatter(x):
        return str(x)

    def float_formatter(x):
        try:
            return '{:.2f}'.format(x)
        except (ValueError, TypeError):
            return None

    def int_formatter(x):
        return str(x)

    d = N(**dtype_dict)

    if d.pandas_type == dex.eml_extract.PandasType.FLOAT:
        return float_formatter
    elif d.pandas_type == dex.eml_extract.PandasType.INT:
        return int_formatter
    elif d.pandas_type == dex.eml_extract.PandasType.CATEGORY:
        return string_formatter
    elif d.pandas_type == dex.eml_extract.PandasType.DATETIME:
        return d.date_fmt_dict['formatter']
    elif d.pandas_type == dex.eml_extract.PandasType.STRING:
        return string_formatter
    else:
        raise AssertionError(f'Invalid PandasType: {d.pandas_type}')


def get_derived_dtypes_from_eml(rid):
    dt_el = dex.eml_cache.get_data_table_el(rid)
    return dex.eml_extract.get_col_attr_list(dt_el)


@dex.cache.disk("parsed-csv", "df")
def get_parsed_csv(rid, eml_ctx):
    return _get_csv(rid, eml_ctx, do_parse=True)


@dex.cache.disk("raw-csv", "df")
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

    Args:
        rid (int): RowID
        eml_ctx (dict):
        do_parse:

    Returns:
        pandas.DataFrame
    """
    csv_stream = dex.obj_bytes.open_csv(rid)

    # Commented lines show the defaults
    arg_dict = dict(
        filepath_or_buffer=csv_stream,
        header=None,  # Do not use column names from the CSV (we get them from the EML)
        names=eml_ctx['pandas_type_dict'].keys(),  # Use column names from EML
        index_col=False,  # Do not use the first column as the index
        # Only get the number of columns that are declared in the EML.
        # This resolves issues where lines have trailing empty columns.
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
        encoding='utf-8',
        encoding_errors='replace',
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
        arg_dict.update(
            dict(
                converters=eml_ctx['parser_dict'],
                # true_values=None,
                # false_values=None,
                # Not required when setting skiprows and skipfooter. Read the number of rows declared in the EML
                # nrows=max_rows,
                na_filter=True,
                na_values=eml_ctx['missing_code_dict'],
                # Add common NaNs:
                # ‘’, ‘#N/A’, ‘#N/A N/A’, ‘#NA’, ‘-1.#IND’, ‘-1.#QNAN’, ‘-NaN’, ‘-nan’, ‘1.#IND’, ‘1.#QNAN’,
                # ‘<NA>’, ‘N/A’, ‘NA’, ‘NULL’, ‘NaN’, ‘n/a’, ‘nan’, ‘null’
                keep_default_na=True,
            )
        )

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

    log.debug('#' * 100)
    log.debug(f'pd.read_csv() kwargs:\n{pprint.pformat(arg_dict)}')
    log.debug(f'pd.read_csv() dialect:\n{pprint.pformat(eml_ctx["dialect"].__dict__)}')
    log.debug('#' * 100)

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

    # csv_df.index.rename('Index', inplace=True)
    csv_df.columns.name = 'Index'

    # The initially generated DF is fragmented and may cause performance warnings.
    # Returning a copy creates a defragmented version of the DF.
    return csv_df.copy()


def apply_nan(df, nan_set: set):
    """Set all values in the df that equal a value in the nan_set to Nan (numpy.nan)"""
    # Add string versions of the various NaN values.
    nan_set.update([str(x) for x in nan_set])
    return df.applymap(lambda x: np.nan if x in nan_set else x)

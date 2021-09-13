import logging
import math
import time

import pandas as pd

import dex.cache
import dex.csv_parser
import dex.obj_bytes
import dex.eml_cache
import dex.eml_types
import dex.exc
import dex.pasta

from flask import current_app as app


log = logging.getLogger(__name__)


@dex.cache.disk("full", "df")
def get_full_csv(rid, **csv_arg_dict):
    try:
        df = _load_csv_to_df(rid, **csv_arg_dict)
        dex.csv_parser.cast_to_eml_types(df, rid)
        return df
    except FileNotFoundError:
        raise dex.exc.RedirectToIndex()


@dex.cache.disk("full-cast", "df")
def get_full_csv_with_eml_cast(rid, **csv_arg_dict):
    df = get_full_csv(rid, **csv_arg_dict)
    dex.csv_parser.cast_to_eml_types(df, rid)
    return df


@dex.cache.disk("head", "df")
def get_csv_head(rid):
    return get_full_csv(rid).head()


@dex.cache.disk("tail", "df")
def get_csv_tail(rid):
    return get_full_csv(rid).tail()


@dex.cache.disk("sample", "df")
def get_csv_sample(rid):
    """Return a DataFrame containing at most `config['CSV_SAMPLE_THRESHOLD']` rows from
    the CSV file.
    """
    df = get_full_csv(rid)
    return df.sample(min(len(df), app.config["CSV_SAMPLE_THRESHOLD"]))


@dex.cache.disk("describe", "df")
def get_description(rid):
    df = get_full_csv(rid)
    return df.describe(
        include="all",
        datetime_is_numeric=True,
    )


@dex.cache.disk("stats", "df")
def get_stats(rid):
    df = get_full_csv(rid)

    name_dict = dict(zip(df.columns, df.columns))
    min_dict = df.min(skipna=True, numeric_only=True)
    max_dict = df.max(skipna=True, numeric_only=True)
    mean_dict = df.mean(skipna=True, numeric_only=True)
    median_dict = df.median(skipna=True, numeric_only=True)
    unique_count = df.nunique()

    # Exclude NA/null values when computing the result.
    # numeric_only: Include only float, int, boolean columns. If None, will attempt to use everything, then use only numeric data. Not implemented for Series.
    # pd.to_numeric(csv_df, errors='coerce').mean()

    stats_df = pd.concat(
        [
            pd.Series(d)
            for d in (
                name_dict,
                min_dict,
                max_dict,
                mean_dict,
                median_dict,
                unique_count,
            )
        ],
        axis=1,
    )
    stats_df.columns = "Column", "Min", "Max", "Mean", "Median", "Unique"
    return stats_df


# Columns


# @dex.cache.disk("ref-col", "list")
# def get_derived_dtypes_list(rid):
#     df = get_description(rid)
#     return df.columns.to_list()


def get_col_series_by_index(rid, col_idx):
    return get_full_csv(rid).iloc[:, col_idx]


def is_nan(x):
    if isinstance(x, (int, float)):
        return math.isnan(x)
    return False


# @dex.cache.disk("cat-cat", "list")
def get_categories_for_column(rid, col_idx):
    """Return a list of the unique values in a categorical column. This assumes that
    the column at the given index is already known to be of type `TYPE_CAT`.
    """
    ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    csv_df = ctx['csv_df']
    col_series = csv_df.iloc[:, int(col_idx)]
    # return list([x for x in col_series.unique() if not is_nan(x)])
    return list(x for x in col_series.unique() if not is_nan(x))


def is_csv(_rid, csv_path):
    try:
        clevercsv_dialect = dex.csv_parser.get_clevercsv_dialect(csv_path)
        count_dict = {}
        with open(csv_path, "r", newline="", encoding='utf-8') as f:
            for i, row in enumerate(clevercsv.reader(f, clevercsv_dialect)):
                if i == 100:
                    break
                count_dict.setdefault(len(row), 0)
                count_dict[len(row)] += 1
        return 1 <= len(count_dict) <= 3
    except Exception:
        return False


#


def _load_csv_to_df(rid, **csv_arg_dict):
    """Read CSV file to DataFrame.

    This uses CleverCSV to determine the dialect of the CSV, then reads it with the
    standard csv module to save time.
    """
    csv_stream = dex.obj_bytes.open_csv(rid)
    dialect = dex.csv_parser.get_dialect(csv_stream)
    skip_rows, header_row = dex.csv_parser.find_header_row(csv_stream)
    log.debug(f"skip_rows: {skip_rows}")
    log.debug(f"pandas.read_csv start: {csv_stream}")
    start_ts = time.time()

    csv_arg_dict.pop("skiprows", None)

    csv_df = pd.read_csv(
        csv_stream,
        dialect=dialect,
        parse_dates=True,
        # parse_dates=['TS'],
        # date_parser=parse_date
        # nrows=1000,
        skiprows=skip_rows,
        **csv_arg_dict,
    )

    log.debug(f"pandas.read_csv: {time.time() - start_ts:.02f}s")
    # log.debug(f"Memory used by DataFrame: {csv_df.memory_usage(deep=True)}")
    return csv_df


def get_dt_df(df):
    """Return a new DataFrame containing only the columns from `df` that contain
    date-times. Columns containing mixed values where more than `fuzz_percent`
    of the values are parsable as date-times, are included. In mixed value columns,
    The NaT (not a time) rows are filled in with interpolated date-times.
    """
    assert isinstance(df, pd.DataFrame)

    dt_df = pd.DataFrame()

    for col_name, col in df.iteritems():
        assert isinstance(col, pd.Series)
        if str(col.dtype).startswith("datetime"):
            col.fillna(method="ffill")
            df[col_name] = col

    return dt_df


# @dex.cache.disk("dt-col", "df")
# def get_dt_col(rid, col_idx):
#     """Return the column with index `col_idx` as a date-time series.
#
#     In mixed value columns, the NaT (not a time) rows are filled in with interpolated
#     date-times.
#     """
#     df = get_csv_sample(rid)
#     dt_series = df.iloc[:, col_idx]
#     dt_series = pd.to_datetime(dt_series, errors="coerce", format=)
#     dt_series.fillna(method=  "ffill")
#     return dt_series


# def get_mixed_columns(df):
#     """Return indexes of columns that have mixed types or missing values"""
#     return [
#         col.index for col, col_type in df.dtypes.iteritems() if col_type == "object"
#     ]

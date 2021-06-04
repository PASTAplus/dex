import logging
import math
import time

import clevercsv
import pandas as pd

import dex.cache
import dex.csv_parser
import dex.csv_tmp
import dex.eml_cache
import dex.eml_types
import dex.exc
import dex.pasta

from flask import current_app as app


log = logging.getLogger(__name__)


@dex.cache.disk("full", "df")
def get_full_csv(rid, **csv_arg_dict):
    try:
        return _load_csv_to_df(rid, **csv_arg_dict)
    except FileNotFoundError:
        raise dex.exc.RedirectToIndex()


@dex.cache.disk("head", "df")
def get_csv_head(rid):
    return get_full_csv(rid).head()


@dex.cache.disk("tail", "df")
def get_csv_tail(rid):
    return get_full_csv(rid).tail()


@dex.cache.disk("sample", "df")
def get_csv_sample(rid):
    """Return a DataFrame containing at most `config.CSV_SAMPLE_THRESHOLD` rows from
    the CSV file.
    """
    df = get_full_csv(rid)
    return df.sample(min(len(df), flask.current_app.config["CSV_SAMPLE_THRESHOLD"]))


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


@dex.cache.disk("ref-col", "list")
def get_ref_col_list(rid):
    df = get_description(rid)
    return df.columns.to_list()


def get_col_name_by_index(rid, col_idx):
    return get_ref_col_list(rid)[col_idx]


def get_col_series_by_index(rid, col_idx):
    return get_full_csv(rid).iloc[:, col_idx]


def is_nan(x):
    if isinstance(x, (int, float)):
        return math.isnan(x)
    return False


# @dex.cache.disk("cat-cat", "list")
def get_categories_for_column(rid, col_idx):
    csv_df = dex.csv_cache.get_full_csv(rid)
    col_series = csv_df.iloc[:, int(col_idx)]
    res_list = pd.Series(col_series.unique())  # .tolist()
    if res_list.dtype == "object":
        res_list = res_list.apply(lambda x: str(x))
    res_list = res_list.sort_values(na_position="last")
    res_list = res_list.fillna("-")
    res_list = res_list.to_list()
    return res_list


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

    This uses clevercsv to determine the dialect of the CSV, then reads it with the
    standard csv module to save time.
    """
    csv_path = dex.csv_tmp.get_data_path_by_row_id(rid)
    dialect = dex.csv_parser.get_dialect(csv_path)
    skip_rows, header_row = dex.csv_parser.find_header_row(csv_path)
    log.debug(f"skip_rows: {skip_rows}")
    log.debug(f"pandas.read_csv start: {csv_path}")
    start_ts = time.time()

    csv_arg_dict.pop("skiprows", None)

    csv_df = pd.read_csv(
        csv_path,
        dialect=dialect,
        parse_dates=True,
        # parse_dates=['TS'],
        # date_parser=parse_date
        # nrows=1000,
        skiprows=skip_rows,
        **csv_arg_dict,
    )

    log.info(f"pandas.read_csv: {time.time() - start_ts:.02f}s")
    # log.info(f"Memory used by DataFrame: {csv_df.memory_usage(deep=True)}")
    return csv_df


def _load_csv_to_df_slow(csv_path):
    """This method handles various broken CSV files but is too slow for use in
    production.
    """
    skip_rows = find_header_row(csv_path)
    log.info(f"skip_rows: {skip_rows}")
    log.info("CleverCSV - read_dataframe start")
    start_ts = time.time()
    csv_df = clevercsv.read_dataframe(csv_path, skiprows=skip_rows)
    log.info(f"CleverCSV - read_dataframe: {time.time() - start_ts:.02f}s")
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


@dex.cache.disk("dt-col", "df")
def get_dt_col(rid, col_idx):
    """Return the column with index `col_idx` as a date-time series.

    In mixed value columns, the NaT (not a time) rows are filled in with interpolated
    date-times.
    """
    df = get_csv_sample(rid)
    dt_series = df.iloc[:, col_idx]
    dt_series = pd.to_datetime(dt_series, errors="coerce")
    dt_series.fillna(method="ffill")
    return dt_series


def get_mixed_columns(df):
    """Return indexes of columns that have mixed types or missing values"""
    return [
        col.index for col, col_type in df.dtypes.iteritems() if col_type == "object"
    ]

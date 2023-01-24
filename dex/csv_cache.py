import logging
import math

import pandas as pd
from flask import current_app as app

import dex.cache
import dex.csv_parser
import dex.eml_cache
import dex.eml_types
import dex.exc
import dex.obj_bytes
import dex.pasta

log = logging.getLogger(__name__)


def get_full_csv(rid):
    eml_ctx = dex.csv_parser.get_eml_ctx(rid)
    return dex.csv_parser.get_parsed_csv(rid, eml_ctx)


@dex.cache.disk("head", "df")
def get_csv_head(rid):
    return get_full_csv(rid).head()


@dex.cache.disk("tail", "df")
def get_csv_tail(rid):
    return get_full_csv(rid).tail()


# @dex.cache.disk("sample", "df")
# def get_csv_sample(rid):
#     """Return a DataFrame containing at most `config['CSV_SAMPLE_THRESHOLD']` rows from
#     the CSV file.
#     """
#     df = get_full_csv(rid)
#     if len(df) > app.config["CSV_SAMPLE_THRESHOLD"]:
#         return df.sample(min(len(df), app.config["CSV_SAMPLE_THRESHOLD"])).sort_index()
#     return df


def get_sample(df):
    """Return a DataFrame containing at most `config['CSV_SAMPLE_THRESHOLD']` rows from
    the CSV file.
    """
    if len(df) > app.config["CSV_SAMPLE_THRESHOLD"]:
        return df.sample(min(len(df), app.config["CSV_SAMPLE_THRESHOLD"]))
    return df


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


def get_col_agg_dict(df, eml_ctx):
    """Calculate per column aggregates for plottable columns"""
    d = {}

    for i, col_name in enumerate(df.columns):
        if not (
            pd.api.types.is_numeric_dtype(df[col_name])
            or pd.api.types.is_datetime64_any_dtype(df[col_name])
        ):
            continue

        try:
            v_max = df.iloc[:, i].max(skipna=True)
            v_min = df.iloc[:, i].min(skipna=True)

            if pd.api.types.is_datetime64_any_dtype(df[col_name]):
                col_dict = eml_ctx['column_list'][i]
                v_max = v_max.strftime('%Y-%m-%d')
                v_min = v_min.strftime('%Y-%m-%d')
            d[i] = dict(col_name=col_name, v_max=v_max, v_min=v_min)
        except Exception:
            log.exception(
                'Exception when calculating per column aggregate for column: {}'.format(
                    df['col_name']
                )
            )

    return d


def get_datetime_col_dict(df):
    return {
        col_name: dict(
            begin_dt=df[col_name].min(skipna=True).strftime('%Y-%m-%d'),
            end_dt=df[col_name].max(skipna=True).strftime('%Y-%m-%d'),
        )
        for col_name in df.columns
        if pd.api.types.is_datetime64_any_dtype(df[col_name])
    }


# @dex.cache.disk("ref-col", "list")
# def get_column_list(rid):
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
    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    col_series = csv_df.iloc[:, int(col_idx)]
    # return list([x for x in col_series.unique() if not is_nan(x)])
    return list(x for x in col_series.unique() if not is_nan(x))


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

import csv
import logging
import time

import clevercsv
import flask
import pandas as pd

import dex.eml_cache
import dex.cache
import dex.pasta
import dex.util

log = logging.getLogger(__name__)


def download_full_csv(data_url, rid):
    """Download or load CSV from disk cache.

    This shares the same cache key as get_full_csv().
    """
    log.debug(f"Downloading CSV: {data_url}")

    with dex.cache.lock(rid, "full", "df"):
        df_path = dex.cache.get_cache_path(rid, "full", "df")
        if df_path.exists():
            return
        with dex.cache.lock(rid, "full", "csv"):
            csv_path = dex.cache.get_cache_path(rid, "full", "csv", mkdir=True)
            if not csv_path.exists():
                dex.pasta.download_data_entity(csv_path, data_url)
            df = _load_csv_to_df(csv_path)
            dex.cache.save_hdf(df_path, "full", df)
        # return df
    # return rid


@dex.cache.disk("full", "df", is_source_obj=True)
def get_full_csv(rid, **csv_arg_dict):
    try:
        return _load_csv_to_df(rid, **csv_arg_dict)
    except FileNotFoundError:
        raise dex.util.RedirectToIndex()


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
    return df.sample(
        min(len(df), flask.current_app.config["CSV_SAMPLE_THRESHOLD"])
    )


@dex.cache.disk("describe", "df")
def get_description(rid):
    df = get_full_csv(rid)
    return df.describe(include="all", datetime_is_numeric=True,)


@dex.cache.disk("stats", "df")
def get_stats(rid):
    df = get_full_csv(rid)

    name_dict = dict(zip(df.columns, df.columns))
    min_dict = df.min(skipna=True, numeric_only=True)
    max_dict = df.max(skipna=True, numeric_only=True)
    mean_dict = df.mean(skipna=True, numeric_only=True)
    median_dict = df.median(skipna=True, numeric_only=True)
    unique_dict = df.nunique()

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
                unique_dict,
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


@dex.cache.disk("cat-col", "list")
def get_categorical_columns(rid):
    """Return list of columns that have fewer than a configurable shreshold percentage
    of unique values. Each item is a tuple with the column index and a descriptive text
    that includes the column name.
    """
    threshold_float = (
        flask.current_app.config["CATEGORY_THRESHOLD_PERCENT"] / 100
    )
    df = get_description(rid)
    cat_list = []
    for i, col_name in enumerate(df.columns):
        cat_col = df.iloc[:, i]
        if "unique" not in cat_col:
            continue
        # Filter out columns with one unique value
        # if cat_col['unique'] == 1:
        #     continue
        unique_float = cat_col["unique"] / cat_col["count"]
        if unique_float < threshold_float:
            cat_list.append(
                (i, f'{col_name} ({cat_col["unique"]:,} categories)')
            )
    return cat_list


#


def _load_csv_to_df(csv_path, **csv_arg_dict):
    """Read CSV file to DataFrame.

    This uses CleverCSV to determine the dialect of the CSV, but then reads it with
    the standard csv module to save time.
    """
    dialect = detect_dialect(csv_path)
    skip_rows = _find_header_row(csv_path)
    log.info(f"skip_rows: {skip_rows}")
    log.info("pandas.read_csv start")
    start_ts = time.time()

    csv_arg_dict.pop("skiprows", None)

    csv_df = pd.read_csv(
        csv_path,
        dialect=get_native_dialect(dialect),
        parse_dates=True,
        # parse_dates=['TS'],
        # date_parser=parse_date
        # nrows=1000,
        skiprows=skip_rows,
        **csv_arg_dict,
    )

    log.info(f"pandas.read_csv: {time.time() - start_ts:.02f}s")
    log.info(f"Memory used by DataFrame: {csv_df.memory_usage(deep=True)}")

    return csv_df


def _load_csv_to_df_slow(csv_path):
    """This method handles various broken CSV files but is too slow for use in
    production.
    """
    skip_rows = _find_header_row(csv_path)
    log.info(f"skip_rows: {skip_rows}")
    log.info("CleverCSV - read_dataframe start")
    start_ts = time.time()
    csv_df = clevercsv.read_dataframe(csv_path, skiprows=skip_rows)
    log.info(f"CleverCSV - read_dataframe: {time.time() - start_ts:.02f}s")
    return csv_df


def _find_header_row(csv_path):
    """Return the index of the first row that looks like the headers in a CSV file.

    This returns the index of the first row that:

        - Has the same number of columns as most of the other rows in the CSV file
        - Does not have pure numbers in any of the fields
        - Does not have any empty fields

    If none of the rows that are checked fill the criteria, returns 0, the index of
    the top row.
    """
    dialect = detect_dialect(csv_path)

    count_dict = {}
    with open(csv_path, "r", newline="") as f:
        for i, row in enumerate(clevercsv.reader(f, dialect)):
            if i == 100:
                break
            count_dict.setdefault(len(row), 0)
            count_dict[len(row)] += 1

    max_row_count = max(count_dict.values())
    for col_count, row_count in count_dict.items():
        if row_count == max_row_count:
            column_count = col_count
            break
    else:
        assert False

    log.debug(f"column_count: {column_count}")

    with open(csv_path, "r", newline="") as f:
        for i, row in enumerate(clevercsv.reader(f, dialect)):
            if i == 100:
                break
            log.debug(f"checking row: {row}")
            if len(row) != column_count:
                continue
            if any(s.isnumeric() for s in row):
                continue
            if any(not s for s in row):
                continue
            return i

    return 0


# def uniform_columns(df):
#     for col_name in df.columns:
#         # df['Price'] = pd.to_numeric(df['Price'])
#         numeric_list = df[df.columns].select_dtypes('number').columns.tolist()
#         df = df.sort_values(numeric_list, ascending=[False] * len(numeric_list))


def detect_dialect(csv_path):
    log.info("CleverCSV - start dialect discovery")
    with open(csv_path, "r", newline="") as fp:
        start_ts = time.time()
        dialect = clevercsv.Sniffer().sniff(
            fp.read(flask.current_app.config["CSV_SNIFF_THRESHOLD"]),
            verbose=True,
        )
        log.info(f"CleverCSV - detect dialect: {time.time() - start_ts:.02f}s")
    return dialect


def get_native_dialect(clever_dialect):
    """Translate from clevercsv.SimpleDialect() to csv.Dialect()"""
    dialect = csv.excel
    dialect.delimiter = clever_dialect.delimiter
    dialect.quotechar = (
        clever_dialect.quotechar if clever_dialect.quotechar else '"'
    )
    dialect.escapechar = (
        clever_dialect.escapechar if clever_dialect.escapechar else None
    )
    return dialect


def get_subsel_through_cache(rid):
    c = get_stats(rid)
    return c.iloc[:, 0:1]


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


# def get_dt_df(df):
#     """Return a new DataFrame containing only the columns from `df` that contain
#     date-times. Columns containing mixed values where more than `fuzz_percent`
#     of the values are parsable as date-times, are included. In mixed value columns,
#     The NaT (not a time) rows are filled in with interpolated date-times.
#     """
#     assert isinstance(df, pd.DataFrame)
#
#     dt_df = pd.DataFrame()
#
#     for col_name, col in df.iteritems():
#         assert isinstance(col, pd.Series)
#         dt_col = pd.to_datetime(col, errors='coerce')
#         nat_count = dt_col.isna().sum()
#         nat_percent = 100 * nat_count / len(df)
#         if (
#             nat_percent
#             <= flask.current_app.config['DATETIME_THRESHOLD_PERCENT']
#         ):
#             dt_col.fillna(method='ffill')
#             dt_df[col_name] = dt_col
#
#     return dt_df
#
# def get_dt_df(df):
#     """Return a new DataFrame containing only the columns from `df` that contain
#     date-times. Columns containing mixed values where more than `fuzz_percent`
#     of the values are parsable as date-times, are included. In mixed value columns,
#     The NaT (not a time) rows are filled in with interpolated date-times.
#     """
#     assert isinstance(df, pd.DataFrame)
#
#     new_dt = pd.DataFrame()
#
#     for col_name, col in df.iteritems():
#         # fmt_str = dateinfer.infer(list(col)[:1000])
#         # log.debug(f'Inferred datetime format: {fmt_str}')
#
#         assert isinstance(col, pd.Series)
#
#         def p(x):
#             # return None
#             log.debug(repr(x))
#             x = str(x)
#             d = dateparser.parse(x)
#             log.debug(f'{x} -> {d}')
#             return d or pd.NaT
#
#         col = col.apply(p)
#         # for i in range(col.size):
#         #     col[i] = dateparser.parse(col[i])
#         # for c in col:
#
#
#         # coerce: Not parsable as date-time is set to NaT (not a time)
#         # dt_df = pd.to_datetime(df, infer_datetime_format=True, errors='coerce')
#         # infer_datetime_format=True,
#         # dt_col = pd.to_datetime(col, format=fmt_str, errors='coerce')
#         # dt_col = col.to_timestamp(errors='coerce')
#
#         # if str(col.dtype).startswith('datetime'):
#         #     new_dt.append(col)
#         # else:
#         nat_count = col.isna().sum()
#         nat_percent = 100 * nat_count / len(df)
#         if (
#             nat_percent
#             <= flask.current_app.config['DATETIME_THRESHOLD_PERCENT']
#         ):
#             col.fillna(method='ffill')
#             new_dt.append(col)
#
#     return new_dt


def get_mixed_columns(df):
    """Return indexes of columns that have mixed types or missing values"""
    return [
        col.index
        for col, col_type in df.dtypes.iteritems()
        if col_type == "object"
    ]


# def get_dt_columns(df):
#     return [
#         col.index
#         for col, col_type in df.dtypes.iteritems()
#         if col_type in ('datetime64',)
#     ]

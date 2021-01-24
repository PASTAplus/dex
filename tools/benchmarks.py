#!/usr/bin/env python

"""Compare and benchmark alternate implementations that we may consider for Dex.
"""

import logging
import random
import string
import sys
import time
import timeit

import pandas as pd

log = logging.getLogger(__name__)

COL_COUNT = 10
# ROW_COUNT = 1 * 1000
ROW_COUNT = 1 * 1000 * 1000

STR_MIN = 1
STR_MAX = 20

CAT_COUNT = 20

# Run each benchmark multiple times for better accuracy
REPEAT_COUNT = 3


def main():
    logging.basicConfig(
        format='%(levelname)-8s %(message)s', level=logging.DEBUG
    )

    cat_list = [[rstr() for i in range(COL_COUNT)] for _ in range(CAT_COUNT)]
    df = create_df_with_categories(cat_list)

    sel_list = []
    sel_list.append((0, set(random.sample(cat_list[0], 2))))

    test_filters(df, sel_list)

    bench_loop_s = (
        timeit.timeit(
            lambda: filter_with_loop(df, sel_list), number=REPEAT_COUNT
        )
        / REPEAT_COUNT
    )
    bench_isin_s = (
        timeit.timeit(
            lambda: filter_with_pandas_isin(df, sel_list), number=REPEAT_COUNT
        )
        / REPEAT_COUNT
    )
    print('#' * 100)
    print(f'columns={COL_COUNT} rows={ROW_COUNT} categories={CAT_COUNT}')
    print(f'Python loop: {bench_loop_s}')
    print(f'Pandas isin: {bench_isin_s}')


def test_filters(df, sel_list):
    """Check that the filter alternatives do the same thing"""
    df_loop = filter_with_loop(df, sel_list)
    df_is_in = filter_with_pandas_isin(df, sel_list)
    assert df_loop.compare(df_is_in).empty


def filter_with_loop(df, sel_list):
    idx_set = set()

    for row_idx, row_series in df.iterrows():

        for col_idx, cat_set in sel_list:
            s = row_series[col_idx]
            if str(s) in cat_set:
                idx_set.add(row_idx)
                break

    df = df.iloc[list(sorted(idx_set))]
    dump(df)
    return df


def filter_with_pandas_isin(df, sel_list):
    for col_idx, cat_set in sel_list:
        df = df.loc[df[col_idx].isin(cat_set)]
    dump(df)
    return df


def create_df_with_categories(cat_list):
    # print('\ncategories:')
    # pprint.pprint(cat_list)
    start_ts = time.time()
    row_list = []
    for i in range(ROW_COUNT):
        row = [random.choice(v) for v in cat_list]
        row_list.append(row)

        if time.time() - start_ts > 1:
            start_ts = time.time()
            print(f'{i:,} / {ROW_COUNT:,} ({i / ROW_COUNT * 100:.02f}%)')
    # Appending rows directly to a DataFrame is very slow. This is much faster.
    df = pd.DataFrame(row_list)
    dump(df)
    return df


def dump(df):
    print('-' * 100)
    print(f'\ndf.info()')
    df.info()
    print(f'\ndf.describe():')
    print(df.describe())


def rstr():
    return ''.join(
        random.sample(string.printable, random.randint(STR_MIN, STR_MAX))
    )


if __name__ == '__main__':
    sys.exit(main())

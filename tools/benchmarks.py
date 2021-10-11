#!/usr/bin/env python

"""Compare and benchmark alternate implementations that we may consider for Dex.
"""
import argparse
import io
import logging
import pprint
import random
import string
import sys
import time
import timeit

import pandas as pd

log = logging.getLogger(__name__)

COL_COUNT = 4
ROW_COUNT = 1 * 100 * 1000

STR_MIN = 1
STR_MAX = 20

CAT_COUNT = 20

# Run each benchmark multiple times for better accuracy
REPEAT_COUNT = 3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Debug level logging',
    )
    args = parser.parse_args()

    logging.basicConfig(
        format='%(levelname)-8s %(message)s',
        level=logging.DEBUG if args.debug else logging.INFO,
        stream=sys.stdout,
    )

    cat_list = create_categories()
    df = create_df_with_categories(cat_list)

    sel_list = create_selection_list(df)
    dump('sel_list', sel_list)

    test_filters(df, sel_list)

    bench_loop_s = (
        timeit.timeit(lambda: filter_with_loop(df, sel_list), number=REPEAT_COUNT) / REPEAT_COUNT
    )
    bench_isin_s = (
        timeit.timeit(lambda: filter_with_isin(df, sel_list), number=REPEAT_COUNT) / REPEAT_COUNT
    )
    bench_query_s = (
        timeit.timeit(lambda: filter_with_query(df, sel_list), number=REPEAT_COUNT) / REPEAT_COUNT
    )
    print('#' * 100)
    print(f'columns={COL_COUNT:,} rows={ROW_COUNT:,} categories={CAT_COUNT:,}')
    print(f'Python loop: {bench_loop_s}')
    print(f'Pandas isin: {bench_isin_s}')
    print(f'Pandas query: {bench_query_s}')


def create_selection_list(df):
    return [
        (str(c), set(random.sample(list(df.loc[:, c]), 2)))
        for c in random.sample(list(df.columns), 2)
    ]


def create_categories():
    """Create a group of random strings for each column in the table."""
    return [
        [
            ''.join(random.choices(string.ascii_lowercase, k=random.randint(STR_MIN, STR_MAX)))
            for _i in range(CAT_COUNT)
        ]
        for _j in range(COL_COUNT)
    ]


def test_filters(df, sel_list):
    """Check that the filter alternatives do the same thing"""
    df_loop = filter_with_loop(df, sel_list)
    df_isin = filter_with_isin(df, sel_list)
    df_query = filter_with_query(df, sel_list)

    dump('loop', df_loop)
    dump('isin', df_isin)
    dump('query', df_query)

    assert df_loop.compare(df_isin).empty
    assert df_loop.compare(df_query).empty


def filter_with_loop(df, sel_list):
    idx_set = set()
    for row_idx, row_series in df.iterrows():
        for col_idx, cat_set in sel_list:
            s = row_series[col_idx]
            if str(s) in cat_set:
                idx_set.add(row_idx)
                break
    df = df.iloc[list(sorted(idx_set))]
    dump('loop', df)
    return df


def filter_with_isin(df, sel_list):
    sel_dict = {k: list(v) for k, v in sel_list}
    df_mask = df.isin(sel_dict).sum(axis=1).astype('bool')
    df = df[df_mask]
    dump('isin', df)
    return df


def filter_with_query(df, sel_list):
    dump('sel_list', sel_list)
    query_str = ' | '.join(
        f'{col_idx} in (' + ','.join(f'"{cat_str}"' for cat_str in cat_set) + ')'
        for col_idx, cat_set in sel_list
    )
    dump('query_str', query_str)
    df = df.query(query_str)
    dump('query', df)
    return df


def dump(msg_str, x):
    if not log.isEnabledFor(logging.DEBUG):
        return

    log.debug('>' * 100)
    log.debug(f'#### {x.__class__.__name__}')
    log.debug(f'#### {msg_str}')

    max_list = [
        ('display.max_columns', 20),
        ('display.max_colwidth', 20),
        ('display.max_info_columns', 200),
        ('display.max_info_rows', 20),
        ('display.max_rows', 200),
        ('display.max_seq_items', 20),
    ]
    with pd.option_context(*tuple(v for x in max_list for v in x)):
        if isinstance(x, pd.DataFrame):
            log.debug('## DataFrame')
            log.debug('# full')
            list(map(log.debug, (x.to_string().splitlines(keepends=False))))
            log.debug(f'## index: {repr(x.index)}')
            log.debug('# info')
            ss = io.StringIO()
            x.info(buf=ss)
            list(map(log.debug, ss.getvalue().splitlines(keepends=False)))
        elif isinstance(x, pd.Series):
            log.debug('## Series')
            log.debug('# full')
            log.debug(x.to_string())
            log.debug(f'# index: {repr(x.index)}')
        else:
            log.debug(pprint.pformat(x))
    log.debug('<' * 100)


def create_df_with_categories(cat_list):
    start_ts = time.time()
    row_list = []
    for i in range(ROW_COUNT):
        row = [random.choice(v) for v in cat_list]
        row_list.append(row)
        if time.time() - start_ts > 1:
            start_ts = time.time()
            print(f'{i:,} / {ROW_COUNT:,} ({i / ROW_COUNT * 100:.02f}%)')
    # Appending rows directly to a DataFrame is very slow. This is much faster.
    df = pd.DataFrame(row_list, columns=list(string.ascii_uppercase)[: len(cat_list)])
    dump('Generated DF with categories', df)
    return df


if __name__ == '__main__':
    sys.exit(main())

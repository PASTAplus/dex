#!/usr/bin/env python

"""Prepopulate caches"""
import collections
import contextlib
import csv
import logging
import os
import pathlib
import sys
import time
import unittest.mock

import pandas as pd
import pandas_profiling.model.base

import pandas_profiling.model.describe

# WALK_ROOT_PATH = '/pasta/data/backup/data1'
WALK_ROOT_PATH = './test_csv'

SOURCE_CSV_PATH = 'sources.csv'
CACHE_CSV_PATH = 'cache2.csv'

SOURCE_FIELD_LIST = ['path', 'size_bytes']
CACHE_FIELD_LIST = [
    'path',
    'plot_count',
    'continuous_variable_count',
    'continuous_variable_list',
]

SourceRow = collections.namedtuple('SourceRow', SOURCE_FIELD_LIST)
CacheRow = collections.namedtuple('CacheRow', CACHE_FIELD_LIST)


log = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        format='%(levelname)-8s %(message)s',
        level=logging.DEBUG,
        stream=sys.stdout,
    )

    # log.debug('debug')
    # log.info('info')
    # log.error('error')

    source_path = pathlib.Path(SOURCE_CSV_PATH)
    if not source_path.exists() or source_path.stat().st_size == 0:
        create_source_csv()

    # with pathlib.Path(CACHE_CSV_PATH).open('w', newline='') as csv_file:
    with DictWriter(
        CACHE_CSV_PATH,
        CACHE_FIELD_LIST,
        row_fn=lambda x: x._asdict(),
        is_write=True,
    ) as cache_writer:
        with DictReader(
            # csv_path, field_list, row_fn, is_write
            SOURCE_CSV_PATH,
            SOURCE_FIELD_LIST,
            row_fn=SourceRow,
            is_write=False,
        ) as source_reader:
            for row_tup in source_reader:
                file_path = pathlib.Path(row_tup.path.strip())
                try:
                    create_cache(file_path, cache_writer)
                except Exception as e:
                    # log.error(f'Error: {file_path}: {repr(e)}')
                    # raise
                    pass


def create_source_csv():
    log.debug('Creating CSV with list of CSV files to process...')
    with DictWriter(
        SOURCE_CSV_PATH,
        SOURCE_FIELD_LIST,
        row_fn=lambda x: x._asdict(),
        is_write=True,
    ) as source_writer:
        for root_dir, dir_list, file_list in os.walk(WALK_ROOT_PATH):
            dir_list[:] = list(sorted(dir_list))
            file_list[:] = list(sorted(file_list))
            for file_name in file_list:
                file_path = pathlib.Path(root_dir, file_name)

                # log.debug(f'Found: {file_path.as_posix()}')
                # log.debug(f'{file_path.suffix.lower()}')

                # if file_path.suffix.lower() != '.csv':
                #     continue

                source_writer.writerow(
                    SourceRow(
                        path=file_path.as_posix(),
                        size_bytes=file_path.stat().st_size,
                    )
                )


def create_cache(csv_path, csv_writer):
    log.debug(f'create_cache() csv_path={csv_path}, csv_writer={repr(csv_writer)}')
    df = pd.read_csv(csv_path)
    # log.debug('-' * 100)
    # log.debug(f'CSV: {csv_path}')
    # '/home/dahl/dev/dex/test-pandas-profiling-interactions-full.csv'
    description_dict = pandas_profiling.model.describe.get_series_descriptions(
        df, unittest.mock.Mock()
    )
    # log.debug(description_dict)
    variables = {column: description["type"] for column, description in description_dict.items()}
    continuous_variable_list = [
        column
        for column, t in variables.items()
        if t == pandas_profiling.model.base.Variable.TYPE_NUM
    ]
    if not continuous_variable_list:
        return
    csv_writer.writerow(
        CacheRow(
            path=csv_path,
            plot_count=len(continuous_variable_list) ** 2,
            continuous_variable_count=len(continuous_variable_list),
            continuous_variable_list=','.join(continuous_variable_list),
        )
    )


def csv_wrapper(csv_class):
    class CSVWrapper(csv_class):
        def __init__(self, csv_path, field_list, row_fn, is_write, *a, **kw):
            self.csv_path = pathlib.Path(csv_path)
            log.info(f'CSV file: {self.csv_path.as_posix()}')

            self.es = contextlib.ExitStack()
            self.f = self.es.enter_context(self.csv_path.open('w' if is_write else 'r', newline=''))

            if is_write:
                kw['fieldnames'] = field_list

            super().__init__(self.f, *a, **kw)

            assert self.fieldnames == field_list, (
                f'CSV is outdated. path="{self.csv_path.as_posix()}" '
                f'expected="{field_list}" actual="{self.fieldnames}"'
            )

            self.field_list = field_list
            self.row_fn = row_fn

            self.start_ts = time.time()
            self.row_count = 0

        def _log_progress(self, row_dict):
            if time.time() - self.start_ts > 1.0:
                self.start_ts = time.time()
                # noinspection PyProtectedMember
                self._dump_dict(self.csv_path.as_posix(), row_dict)
                log.info(f'  row count: {self.row_count}')

        def _dump_dict(self, title_str, d):
            log.info(title_str)
            # noinspection PyProtectedMember
            list(
                map(
                    log.info,
                    [f'  {k}: {v}' for (k, v) in d.items()],
                )
            )

    return CSVWrapper


@csv_wrapper
class DictReader(csv.DictReader):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __next__(self):
        row_dict = super().__next__()
        row_tup = self.row_fn(**row_dict)
        self.row_count += 1
        self._log_progress(row_dict)
        # log.debug(f'__next__() {row_tup}')
        return row_tup


@csv_wrapper
class DictWriter(csv.DictWriter):
    def __enter__(self):
        self.writeheader()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def writeheader(self):
        header = dict(zip(self.fieldnames, self.fieldnames))
        super().writerow(header)

    def writerow(self, row_tup):
        # log.debug(f'writerow() {row_tup}')
        # noinspection PyProtectedMember
        row_dict = self.row_fn(row_tup)
        super().writerow(row_dict)
        self.row_count += 1
        self._log_progress(row_dict)


if __name__ == '__main__':
    sys.exit(main())

#!/usr/bin/env python

"""Check if file is CSV"""
import csv
import logging
import os
import pathlib
import random
import re
import shutil
import string
import subprocess
import sys
import urllib.parse

MAX_READ_LEN = 4096
CSV_COUNT = 100
PKG_PATH = pathlib.Path('./pkg').resolve()
PKG_TXT_PATH = pathlib.Path('./pkg.txt').resolve()
DST_PATH = pathlib.Path('../csv').resolve()

DATA_RX = re.compile(
    """
    (?P<scope_str>[^/]+)\.
    (?P<id_str>\d+)\.
    (?P<ver_str>\d+)/
    (?P<entity_str>[a-f0-9A-F]{32})
    $
    """,
    re.VERBOSE,
)


log = logging.getLogger(__name__)


def main():
    logging.basicConfig(
        format='%(levelname)-8s %(message)s', level=logging.DEBUG
    )
    # file_list = pathlib.Path('all-data').read_text().splitlines(keepends=False)

    file_list = get_path_list()
    random.shuffle(file_list)

    dst_root_path = pathlib.Path('../csv')
    shutil.rmtree(dst_root_path, ignore_errors=True)
    dst_root_path.mkdir(exist_ok=True)

    found_count = 0
    for path_str in file_list:
        if found_count == CSV_COUNT:
            break

        m = DATA_RX.search(path_str[len(PKG_PATH.as_posix()):])
        if not m:
            continue

        src_path = pathlib.Path(path_str)

        if not is_csv(src_path):
            continue

        d = dict(m.groupdict())
        print(d['scope_str'], int(d["id_str"]), int(d["ver_str"]), d['entity_str'])

        pkg_id = '.'.join([d['scope_str'], d["id_str"], d["ver_str"]])
        dst_dir_path = pathlib.Path(DST_PATH, pkg_id)
        dst_dir_path.mkdir(parents=True, exist_ok=True)


        dst_path = dst_dir_path / d['entity_str']

        # dst_path = dst_root_path / urllib.parse.quote(pkg_id, safe='')

        log.info(f'   {src_path.as_posix()}')
        log.info(f'-> {dst_path.as_posix()}')
        shutil.copy(src_path, dst_path)
        os.chmod(dst_path, 0o644)

        # Include EML docs
        for eml_name in ('Level-0-EML.xml', 'Level-1-EML.xml'):
            eml_path = pathlib.Path(src_path.parent, eml_name)
            if eml_path.exists():
                shutil.copy(eml_path, dst_dir_path)


        # subprocess.run(['pixz', dst_path.as_posix()])
        # # , dst_path.as_posix() + '.xz'
        # os.unlink(dst_path)

        found_count += 1


def get_path_list():
    if not PKG_TXT_PATH.exists():
        log.info('Creating path cache...')
        with PKG_TXT_PATH.open('w') as f:
            for root_path, dir_list, file_list in os.walk(PKG_PATH):
                for file_name in file_list:
                    file_path = pathlib.Path(root_path, file_name)
                    f.write(f'{file_path.resolve().as_posix()}\n')
    return PKG_TXT_PATH.read_text().splitlines(keepends=False)


def is_csv(check_path):
    log.info(check_path.as_posix())

    try:
        head_str = check_path.open(encoding='utf-8').read(MAX_READ_LEN)
    except UnicodeDecodeError:
        return False

    head_list = head_str.splitlines(keepends=False)[:-1]
    head_str = '\n'.join(head_list)

    if not all(c in string.printable or c.isprintable() for c in head_str):
        return False

    try:
        dialect = csv.Sniffer().sniff(head_str)
    except csv.Error:
        return False

    log.debug(f'Dialect:')
    log.debug(f'  doublequote={dialect.doublequote}')
    log.debug(f'  delimiter={dialect.delimiter}')
    log.debug(f'  quotechar={dialect.quotechar}')
    log.debug(f'  skipinitialspace={dialect.skipinitialspace}')

    if dialect.delimiter not in (',', '\t'):
        return False

    len_set = set()
    for row in csv.reader(head_list, dialect=dialect):
        len_set.add(len(row))

    if len(len_set) > 2:
        return False

    return True


if __name__ == '__main__':
    sys.exit(main())

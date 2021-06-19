import logging
import multiprocessing
import multiprocessing.pool
import os
import random
import threading

# import threading
import time

import fasteners

import dex.util

log = logging.getLogger(__name__)

# {'delimiter': ',',
#  'doublequote': True,
#  'escapechar': None,
#  'lineterminator': '\r\n',
#  'quotechar': '"',
#  'quoting': 0,
#  'skipinitialspace': False}


def test_0000():
    pool = multiprocessing.pool.Pool(processes=20)

    for i in range(20):
        # pool.apply_async(proc)
        pool.apply_async(proc3)

    pool.close()
    pool.join()


def p(msg):
    log.debug(f'Start {threading.get_native_id()} {os.getpid()}: {msg}')
    time.sleep(random.random() * 3)
    log.debug(f'End   {threading.get_native_id()} {os.getpid()}: {msg}')


@fasteners.interprocess_locked('/tmp/twet')
def proc3():
    for i in range(3):
        p("proc3 - process")
        p(f'Creating thread {i}')
        threading.Thread(
            target=thread4, args=(i, f'from-proc3-{i}'), name=f'thread-{i}'
        ).run()


# @fasteners.interprocess_locked('/tmp/twet')
def thread4(*args):
    # for i in range(3):
    #     p(f'proc4 - thread: {args}')
    lock = dex.util.Lock('testlock')
    with lock(123, 'test_key', 'test_obj'):
        p('1')

    # lock = util.Lock('testlock')
    # p('1')
    # with lock(123, 'test_key', 'test_obj'):
    #     p('1')
    #     with lock(123, 'test_key', 'test_obj'):
    #         p('2')
    #         with lock(123, 'test_key', 'test_obj'):
    #             p ('3')
    #             with lock(123, 'test_key', 'test_obj'):
    #                 p ('4')


def proc2():
    # lock  = util.Lock('testlock')
    # with lock(123, 'test_key', 'test_obj'):
    #     with lock(123, 'test_key', 'test_obj'):
    #         with lock(123, 'test_key', 'test_obj'):
    p(f'{threading.current_thread().ident} {os.getpid()}')


def proc():
    lock = dex.util.Lock('testlock')
    with lock(123, 'test_key', 'test_obj'):
        with lock(123, 'test_key', 'test_obj'):
            with lock(123, 'test_key', 'test_obj'):
                time.sleep(100)

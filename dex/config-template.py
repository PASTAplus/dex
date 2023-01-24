import logging.config
import pathlib

import sys
import tempfile

# Logging

DEBUG = FLASK_DEBUG = True

print('Setting up logging (config)...', file=sys.stderr)

logging.config.dictConfig(
    {
        'version': 1,
        'formatters': {
            'default': {
                'format': '%(asctime)s %(process)d %(name)s %(levelname)-8s %(message)s',
                'datefmt': '%Y-%m-%d %H:%M:%S',
            },
        },
        'handlers': {
            'console': {
                'class': "logging.StreamHandler",
                'stream': 'ext://sys.stderr',
                'formatter': 'default',
            }
        },
        'loggers': {
            # The root logger is configured like regular loggers except that the `propagate`
            # setting is not applicable.
            '': {
                'handlers': ['console'],
                'level': logging.DEBUG if DEBUG else logging.INFO,
            },
            # Increase logging level on loggers that are noisy at debug level
            'requests': {
                'level': logging.INFO,
            },
            'urllib3': {
                'level': logging.INFO,
            },
            'matplotlib': {
                'level': logging.INFO,
            },
            # Flask prints the full list of URL query params when in development mode. The
            # DataTable widget sends huge queries, so to reduce the noise, we increase the
            # logging level for Flask's logger here, and write a brief log record with just
            # the URL in a 'before_request' handler.
            'werkzeug': {
                'level': logging.INFO,
            },
        },
    }
)

SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

log = logging.getLogger(__name__)

ROOT_PATH = pathlib.Path(__file__, "../..").resolve()

PASTA_BASE_URL = 'https://pasta.lternet.edu/package'


# PASTA_BASE_URL = "https://pasta-d.lternet.edu/package"


def first_existing(*path_tup, is_file=False):
    """Given a list of paths, returns the first one that is an existing dir or file
    on the local system. The returned path is resolved to absolute.

    Args:
        *path_tup (pathlib.Path or str):
            List of paths. The paths must be absolute, or relative to the root directory
            of the DeX project.
        is_file (bool):
            False: Path must be to an existing dir
            True: Path must be to an existing file

    Returns:
        pathlib.Path: The first path that is for an existing dir or file (depending
        on the setting of the `is_dir` parameter.
    """
    path_tup = [ROOT_PATH / pathlib.Path(p).resolve() for p in path_tup]
    for p in path_tup:
        # This function runs at import time, so we skip logging here in
        # order to not trigger early log setup.
        if p.is_dir():
            if is_file:
                raise AssertionError(f'Found dir path, but expected file path: {p.as_posix()}')
            log.info(f'Using valid dir path: {p.as_posix()}')
            return p
        elif p.is_file():
            if not is_file:
                raise AssertionError(f'Found file path, but expected dir path: {p.as_posix()}')
            log.info(f'Using valid file path: {p.as_posix()}')
            return p
        elif p.exists():
            assert False, f'Path exists, but does not reference a dir or file: {p.as_posix()}'

    assert False, "None of the provided alternate paths exist: {}".format(
        ', '.join(p.as_posix() for p in path_tup)
    )


STATIC_PATH = first_existing("dex/static")
TMP_PATH = pathlib.Path(tempfile.gettempdir())

# Caching

# For debugging, disk caching can be disabled. The cached versions will still be updated.
DISK_CACHE_ENABLED = True

# Temporary cache
TMP_CACHE_ROOT = TMP_PATH / 'dex-tmp-cache'
TMP_CACHE_LIMIT = 10
TMP_CACHE_ROOT.mkdir(exist_ok=True, parents=True)

# Permanent cache
CACHE_ROOT_DIR = first_existing(
    "../dex-cache",
    '/home/pasta/dex-cache',
)

# Paths to search for sample CSV files.
CSV_ROOT_DIR = first_existing(
    # Production
    '/pasta/data/backup/data1',
    # Dev / Staging
    '/home/dahl/dev/dex-data/___samples',
    '/home/pasta/dex-samples',
    # For environments in which no CSV files are available in the local filesystem,
    # point to an empty dir.
    '/var/empty',
)

# Number of attempts at reading / generating a value through the cache before
# raising an exception. If set to `3`, the caching will suppress the first two exceptions
# that occur, and will raise the 3rd exception if it occurs.
CSV_CACHE_TRIES = 3

PROFILING_CONFIG_PATH = first_existing('dex/profiling_config.yml', is_file=True)

SECRET_KEY = "SECRET_KEY"

PROD = "pasta.lternet.edu"
STAGE = "pasta-s.lternet.edu"
DEV = "pasta-d.lternet.edu"

# HOST = "http://dex.edirepository.org"
HOST = 'http://127.0.0.1:5000'

SQLITE_PATH = first_existing("sqlite.db", is_file=True)

STALE = 10

# If these values are changed, the cached dataframes must be cleared for the new values
# to take effect.

# Max number of cells to read from CSV file. This prevents running out of memory on
# really large CSV files.
CSV_MAX_CELLS = 50_000_000

# Threshold at which we switch from processing all rows in a CSV file and instead
# process only a sample of the rows. Effectively, the number of rows that are processed
# for most functionality is capped at this value.
CSV_SAMPLE_THRESHOLD = 100

# Max number of rows to examine when analyzing CSV files to determine format and content.
CSV_SNIFFER_THRESHOLD = 100

# Set of cell values implicitly interpreted as NaN in CSV files.
# There are in addition to those declared in EML documents.
CSV_NAN_SET = {
    '',
    '#N/A',
    '#N/A',
    'N/A',
    '#NA',
    '-1.#IND',
    '-1.#QNAN',
    '-NaN',
    '-nan',
    '1.#IND',
    '1.#QNAN',
    '<NA>',
    'N/A',
    'NA',
    'NULL',
    'NaN',
    'n/a',
    'nan',
    'null',
}

# Number of bytes in each chunk data in streamed responses.
CHUNK_SIZE_BYTES = 8192

# Pygments style for XML syntax highlighting
EML_STYLE_NAME = 'perldoc'

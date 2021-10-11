import logging
import pathlib
import sys
import tempfile

DEBUG = FLASK_DEBUG = True
DEBUG_PANEL = DEBUG

SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

try:
    import uwsgi

    RUNNING_UNDER_UWSGI = True
except ImportError:
    RUNNING_UNDER_UWSGI = False

log = logging.getLogger(__name__)

ROOT_PATH = pathlib.Path(__file__, "../..").resolve()


def first_existing(*path_tup, is_file=False):
    """Given a list of paths, returns the first one that is an existing dir or file
    on the local system. The returned path is resolved to absolute.

    Args:
        *path_tup (pathlib.Path or str):
            List of paths. The paths must be absolute, or relative to the root directory
            of the Dex project.
        is_file (bool):
            False: Path must be to an existing dir
            True: Path must be to an existing file

    Returns:
        pathlib.Path: The first path that is for an existing dir or file (depending
        on the setting of the `is_dir` parameter.
    """
    for p in path_tup:
        p = (ROOT_PATH / pathlib.Path(p)).resolve()
        # This function runs at import time, so we skip logging here in
        # order to not trigger early log setup.
        if p.is_dir():
            assert not is_file, f'Found dir path, but expected file path: {p.as_posix()}'
            return p
        elif p.is_file():
            assert is_file, f'Found file path, but expected dir path: {p.as_posix()}'
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
    '/home/dahl/dev/dex-data/___data',
    '/pasta/data/backup/data1',
    '/home/pasta/pkg',
    '/home/pasta/___samples',
    '/home/dahl/dev/dex-data/___samples',
    'csv',
)

# Number of attempts at reading / generating a value through the cache before
# raising an exception. If set to `3`, the caching will suppress the first two exceptions
# that occur, and will raise the 3rd exception if it occurs.
CSV_CACHE_TRIES = 3

# When running under uWSGI, the path to the python interpreter binary in the venv is not
# discoverable.
if RUNNING_UNDER_UWSGI:
    PYTHON_BIN = "/home/pasta/.pyenv/versions/dex_3_9_5/bin/python"
else:
    PYTHON_BIN = sys.executable

PROFILING_SH_PATH = first_existing("dex/profiling_proc.sh", is_file=True)
PROFILING_PY_PATH = first_existing("dex/profiling_proc.py", is_file=True)
# PROFILING_LOGS_PATH = ROOT_PATH / '..' / "dex-logs/profiling.log"
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

# Threshold at which we switch from processing all rows in a CSV file and instead
# process only a sample of the rows. Effectively, the number of rows that are processed
# for most functionality is capped at this value.
CSV_SAMPLE_THRESHOLD = 10000

# Max number of rows to examine when analyzing CSV files to determine format and content.
CSV_SNIFFER_THRESHOLD = 1000

# Number of bytes in each chunk data in streamed responses.
CHUNK_SIZE_BYTES = 8192

# Pygments style for XML syntax highlighting
EML_STYLE_NAME = 'perldoc'

LOG_CONFIG = {
    'version': 1,
    'formatters': {
        'default': {
            'format': '%(asctime)s %(process)d %(levelname)8s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'stdout': {
            'class': "logging.StreamHandler",
            'stream': 'ext://sys.stdout',
            'formatter': 'default',
        }
    },
    'root': {},
    'loggers': {
        '': {
            'handlers': ['stdout'],
            # 'handlers': ['console'],
            # 'level': logging.DEBUG,
            'propagate': True,
            'level': 'DEBUG',  # 'INFO'
        },
        # Increase logging level on loggers that are noisy at debug level.
        'matplotlib': {
            'level': 'ERROR',
        },
    },
}

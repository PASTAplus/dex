import logging
import logging.config
import pathlib
import tempfile

HOME_PATH = pathlib.Path.home()
HERE_PATH = pathlib.Path(__file__).resolve().parent
ROOT_PATH = pathlib.Path(__file__, "../..").resolve()
TMP_PATH = pathlib.Path(tempfile.gettempdir())

# Logging
DEBUG = FLASK_DEBUG = DEBUG_PANEL = True
LOG_LEVEL = logging.DEBUG if DEBUG else logging.INFO
# LOG_LEVEL = logging.ERROR

logging.config.dictConfig(
    {
        'version': 1,
        # The root logger is configured like regular loggers except that the `propagate`
        # setting is not applicable.
        'root': {
            'handlers': ['stdout', 'file'],
            'level': LOG_LEVEL,
        },
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
            },
            'file': {
                'class': 'logging.FileHandler',
                'filename': (HOME_PATH / 'dex.log').as_posix(),
                'formatter': 'default',
            },
        },
        # The root logger is configured like regular loggers except that the `propagate`
        # setting is not applicable.
        'loggers': {
            # Increase logging level on loggers that are noisy at debug level
            'requests': {
                'level': 'INFO',
            },
            'urllib3': {
                'level': 'INFO',
            },
            # Flask prints the full list of URL query params when in development mode. The
            # DataTable widget sends huge queries, so to reduce the noise, we increase the
            # logging level for Flask's logger here, and write a brief log record with just
            # the URL in a 'before_request' handler.
            'werkzeug': {
                'level': 'INFO',
            },
        },
    }
)

log = logging.getLogger(__name__)

# Flask
# https://flask.palletsprojects.com/en/2.3.x/config/
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
SECRET_KEY = "SECRET_KEY"

PASTA_BASE_URL = "https://pasta-d.lternet.edu/package"

STATIC_PATH = HERE_PATH / 'static'
assert STATIC_PATH.is_dir()

# Caching

# For debugging, disk caching can be disabled. The cached versions will still be updated.
DISK_CACHE_ENABLED = True

# Temporary cache
TMP_CACHE_ROOT = TMP_PATH / 'dex-tmp-cache'
TMP_CACHE_LIMIT = 100

# Permanent cache
CACHE_ROOT_DIR = ROOT_PATH / '../../dex-cache'
assert STATIC_PATH.is_dir()

# Path to search for locally stored packages
# For environments in which no CSV files are available in the local filesystem, point to an empty dir.
LOCAL_PACKAGE_ROOT_DIR = pathlib.Path('/var/empty')
assert STATIC_PATH.is_dir()

# Path to search for locally stored sample packages
# For environments in which no CSV files are available in the local filesystem, point to an empty dir.
LOCAL_SAMPLE_ROOT_DIR = ROOT_PATH / '../../dex-samples'
assert STATIC_PATH.is_dir()

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

PROFILING_CONFIG_PATH = ROOT_PATH / 'dex/profiling_config.yml'

SQLITE_PATH = ROOT_PATH / 'sqlite.db'
assert SQLITE_PATH.is_file()

# Max number of cells to read from CSV file. This prevents running out of memory on
# really large CSV files.
CSV_MAX_CELLS = 50_000_000

# If these values are changed, the cached dataframes must be cleared for the new values
# to take effect.

# Threshold at which we switch from processing all rows in a CSV file and instead
# process only a sample of the rows. Effectively, the number of rows that are processed
# for most functionality is capped at this value.
# TODO: This should use CSV_MAX_CELLS
CSV_SAMPLE_THRESHOLD = 10000

# Number of bytes in each chunk data in streamed responses.
CHUNK_SIZE_BYTES = 8192

# Pygments style for XML syntax highlighting
EML_STYLE_NAME = 'perldoc'

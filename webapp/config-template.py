import pathlib
import sys

FLASK_DEBUG = False

SESSION_COOKIE_SECURE = True
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"

try:
    import uwsgi

    RUNNING_UNDER_UWSGI = True
except ImportError:
    RUNNING_UNDER_UWSGI = False

# For debugging, disk caching can be disabled.
DISK_CACHE_ENABLED = False

ROOT_PATH = pathlib.Path(__file__, "../..").resolve()
STATIC_PATH = ROOT_PATH / "webapp/static"
CSV_ROOT_DIR = ROOT_PATH / "csv"
CACHE_ROOT_DIR = ROOT_PATH / ".." / "cache"

# When running under uWSGI, the path to the python interpreter binary in the venv is not
# discoverable.
if RUNNING_UNDER_UWSGI:
    PYTHON_BIN = "/home/pasta/anaconda3/envs/dex2/bin/python"
else:
    PYTHON_BIN = sys.executable

SECRET_KEY = "SECRET_KEY"

PROD = "pasta.lternet.edu"
STAGE = "pasta-s.lternet.edu"
DEV = "pasta-d.lternet.edu"

HOST = "http://dex.edirepository.org"
# HOST = 'http://127.0.0.1:5000'

SHELVE_PATH = ROOT_PATH / "perf.db"
SQLITE_PATH = ROOT_PATH / "sqlite.db"

STALE = 10

# If these values are changed, the cached dataframes must be cleared for the new values
# to take effect.

# Threshold at which we switch from processing all rows in a CSV file and instead
# process only a sample of the rows. Effectively, the number of rows that are processed
# for most functionality is capped at this value.
CSV_SAMPLE_THRESHOLD = 10000

# Max number of rows to examine when analyzing CSV files to determine format and content.
CSV_SNIFFER_THRESHOLD = 1000

import shelve

from flask import current_app as app


def set(k, v):
    with shelve.open(app.config["SHELVE_PATH"].as_posix()) as db:
        db[k] = v


def get(k):
    with shelve.open(app.config["SHELVE_PATH"].as_posix()) as db:
        return db.get(k)

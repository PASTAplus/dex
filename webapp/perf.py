import flask
import shelve


def set(k, v):
    with shelve.open(flask.current_app.config["SHELVE_PATH"].as_posix()) as db:
        db[k] = v


def get(k):
    with shelve.open(flask.current_app.config["SHELVE_PATH"].as_posix()) as db:
        return db.get(k)

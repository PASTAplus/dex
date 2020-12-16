import collections
import json
import logging
import re
import sqlite3

import flask

import dex.pasta
import dex.util

log = logging.getLogger(__name__)


def init_db():
    with flask.current_app.app_context():
        db = get_db()
        with flask.current_app.open_resource("schema.sql", mode="r") as f:
            db.cursor().executescript(f.read())
        db.commit()


def namedtuple_factory(cursor, row):
    """
    Usage:
    con.row_factory = namedtuple_factory
    """
    fields = [col[0] for col in cursor.description]
    Row = collections.namedtuple("Row", fields)
    return Row(*row)


def get_db():
    db = getattr(flask.g, "_database", None)
    if db is None:
        db = flask.g._database = sqlite3.connect(
            flask.current_app.config["SQLITE_PATH"].as_posix()
        )

    # Return namedtuples.
    db.row_factory = namedtuple_factory
    return db


def query_db(query, args=(), one=False):
    """Query and return rows.
    Typically used for select queries.

    Args:
        query:
        args:
        one (bool): True: If the query does not return exactly one row, raise OneError.
          If it does return one row, return it as a namedtuple.
    """
    cur = get_db().execute(query, args)
    try:
        rv = cur.fetchall()
    finally:
        cur.close()
    log.debug(
        'query_db() query="{}", args="{}", rv={} bool(rv)={}'.format(
            re.sub(r"(\n|\r|\s| )+", " ", query), args, rv, bool(rv)
        )
    )
    if one:
        if len(rv) != 1:
            raise OneError(
                f"Expected query to return exactly one row, not {len(rv)} rows",
                len(rv),
            )
        return rv[0]
    return rv


def query_id(query, args=()):
    """Query with automatic commit and return last row ID.
    Typically used for insert and update queries.
    """
    connection = get_db()
    try:
        cur = connection.execute(query, args)
        # rv = cur.fetchall()
        row_id = cur.lastrowid
        cur.close()
        return row_id
    finally:
        connection.commit()


def add_entity(data_url):
    """Parse the data_url into package elements, stores the elements and return the id of the new row."""
    entity_tup = dex.pasta.parse_pasta_data_url(data_url)
    return query_id(
        """
        replace into entity
        (data_url, base_url, scope, identifier, version, entity) 
        values (?, ?, ?, ?, ?, ?)
    """,
        entity_tup,
    )


def get_entity(row_id):
    try:
        row_tup = query_db(
            """
            select data_url, base_url, scope, identifier, version, entity
            from entity where id = ?
        """,
            (row_id,),
            one=True,
        )
    except OneError:
        raise dex.util.RedirectToIndex(f"Unknown Package")
    entity_tup = dex.pasta.EntityTup(*row_tup)
    log.debug(f'get_entity() row_id={row_id} entity_tup="{entity_tup}"')
    return entity_tup


def get_entity_as_dict(row_id):
    """Wraps namedtuple._asdict() since the method has a leading underscore even though
    it's a documented part of the public API.
    """
    # noinspection PyProtectedMember
    return get_entity(row_id)._asdict()


def get_data_url(row_id):
    return get_entity(row_id).data_url


class UnknownEntity(Exception):
    pass


class OneError(Exception):
    """A query was run with `one=True` but did not return exactly one row"""

    def __init__(self, msg, row_count):
        super(OneError, self).__init__(msg)
        self.row_count = row_count

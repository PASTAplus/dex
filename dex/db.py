import collections
import logging
import sqlite3

import flask
from flask import current_app as app

import dex.exc
import dex.pasta

log = logging.getLogger(__name__)


def init_db():
    with flask.current_app.app_context():
        db = get_db()
        with flask.current_app.open_resource("schema.sql", mode="r") as f:
            db.cursor().executescript(f.read())
        db.commit()


def clear_entities():
    """Delete all rows in the entity table. Used together with clearing the
    filesystem caches, to force everything to be reprocessed.
    """
    log.debug('Clearing entities from DB')
    cnx = get_db()
    try:
        row_list = query_db("""delete from entity;""", (), db=(cnx))
        log.debug(row_list)
        # cur.fetchall()
        # cur.close()
    finally:
        cnx.commit()


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
        db = flask.g._database = sqlite3.connect(app.config["SQLITE_PATH"].as_posix())

    # Return namedtuples.
    db.row_factory = namedtuple_factory
    return db


def query_db(query, args=(), one=False, db=None):
    """Query and return rows.
    Typically used for select queries.

    Args:
        query:
        args:
        one (bool): True: If the query does not return exactly one row, raise OneError.
          If it does return one row, return it as a namedtuple.
    """
    cur = (db or get_db()).execute(query, args)
    try:
        rv = cur.fetchall()
    finally:
        cur.close()
    # log.debug(
    #     'query_db() query="{}", args="{}", rv={} bool(rv)={}'.format(
    #         re.sub(r"(\n|\r|\s| )+", " ", query), args, rv, bool(rv)
    #     )
    # )
    if one:
        if len(rv) != 1:
            raise OneError(
                f"Expected query to return exactly one row, not {len(rv)} rows",
                len(rv),
            )
        return rv[0]
    return rv


def query_id(query, args=(), db=None):
    """Query with automatic commit and return last row ID.
    Typically used for insert and update queries.
    """
    cnx = db or get_db()
    try:
        cur = cnx.execute(query, args)
        row_id = cur.lastrowid
        cur.close()
        return row_id
    finally:
        cnx.commit()


def add_entity(data_url):
    """Parse the data_url into package elements, stores the elements and return the id
    of the new row.
    """
    entity_tup = dex.pasta.get_entity_by_data_url(data_url)
    # 'replace into' causes the id to increase, which could break existing URLs. We want
    # the id to remain constant, so have to do things in a bit of a cumbersome way here.
    cnx = get_db()
    try:
        row_id = query_db(
            """select id from entity e where data_url = ?""",
            (entity_tup.data_url,),
            db=cnx,
        )
        if row_id:
            return row_id[0].id
        return query_id(
            """
            insert into entity
            (data_url, base_url, scope, identifier, version, entity) 
            values (?, ?, ?, ?, ?, ?)
            """,
            entity_tup,
            db=cnx,
        )
    finally:
        cnx.commit()


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
        raise dex.exc.RedirectToIndex(f"Unknown Package")

    # TODO: Determine how we should handle moving between pasta and pasta-d.
    # row_list = list(row_tup)
    # row_list[1] = row_list[1].replace('pasta-d', 'pasta')
    entity_tup = dex.pasta.EntityTup(*row_tup)
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

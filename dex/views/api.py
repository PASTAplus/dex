"""View for APIs
"""
import logging

import flask
import requests

import dex.cache
import dex.csv_cache
import dex.db
import dex.debug
import dex.eml_cache
import dex.filesystem
import dex.obj_bytes
import dex.pasta

log = logging.getLogger(__name__)

api_blueprint = flask.Blueprint("api", __name__, url_prefix="/dex/api")


@api_blueprint.route("/preview", methods=['OPTIONS'])
def preview_cors_preflight():
    return (
        '',
        204,
        {
            # 'Access-Control-Allow-Origin': '*',
            # 'Access-Control-Allow-Methods': 'POST',
            # 'Access-Control-Allow-Headers': 'Content-Type',
        },
    )


@api_blueprint.route("/preview", methods=['POST'])
def preview():
    if not flask.request.is_json:
        return "Request must be JSON", 400
    d = flask.request.json
    if not {'eml', 'csv', 'dist'}.issubset(d.keys()):
        return 'Request JSON must contain keys "eml", "csv" and "dist"', 400
    if not _check_url(d['eml']):
        return f'EML URL is not accessible: {d["eml"]}', 400
    if not _check_url(d['csv']):
        return f'CSV URL is not accessible: {d["csv"]}', 400

    rid = dex.db.add_entity(d['dist'], d['eml'], d['csv'])

    # For testing, flush cache before processing the new preview
    # dex.cache.flush_cache(rid)

    log.debug(f'Registered new preview. rid="{rid}"')

    return str(rid), 200


@api_blueprint.route("/preview", methods=['DELETE'])
def invalidate_cache_for_entity():
    if not flask.request.is_json:
        return "Request must be JSON", 400
    d = flask.request.json
    if 'dist' not in d.keys():
        return 'Request JSON must contain a "dist" key', 400
    dist_url = d['dist']

    rid = dex.db.get_rid_by_dist_url(dist_url)
    if not rid:
        return f'Unknown distribution URL: {dist_url}', 200

    dex.cache.flush_cache(rid)
    dex.obj_bytes.invalidate(rid)

    log.debug(f'Invalidated cache for dist_url. dist_url="{dist_url}"')

    return f'Invalidated caches for distribution URL: {dist_url}', 200


def _check_url(url):
    """Check if we can access the provided URL"""
    response = requests.head(url)
    return response.status_code == 200

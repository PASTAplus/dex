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
import dex.exc
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


@api_blueprint.route("/package/<path:package_id>", methods=["DELETE"])
def flush_cache_for_package(package_id):
    package_deleted = False
    try:
        for rid in dex.db.get_rid_list_by_package_id(package_id):
            log.info(
                f'Flushing cache files and DB for package. '
                f'package_id="{package_id}" rid="{rid}"'
            )
            dex.cache.flush_cache(rid)
            # dex.db.drop_entity(rid)
            package_deleted = True
    except dex.exc.DexError as e:
        log.error(f'Error when flushing cache for package. package_id="{package_id}": {str(e)}')
        return str(e), 400
    if not package_deleted:
        msg, status_code = f'Package not found. package_id="{package_id}"', 404
    else:
        msg, status_code = (
            f'Successfully flushed cache for package. package_id="{package_id}"'
        ), 200
    log.info(msg)
    return msg, status_code


def _check_url(url):
    """Check if we can access the provided URL"""
    response = requests.head(url)
    return response.status_code == 200

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

    # preview_key = gen_preview_key(d)
    rid = dex.db.add_entity(d['dist'], d['eml'], d['csv'])

    dex.cache.flush_cache(rid)

    log.error(f'rid="{rid}"')

    return str(rid), 200
    # return flask.redirect(f'/dex/profile/{rid}')


def _check_url(url):
    """Check if we can access the provided URL"""
    response = requests.head(url)
    return response.status_code == 200


# def gen_preview_key(preview_dict):
#     hasher = hashlib.sha1()
#     for s in 'eml', 'csv', 'entity':
#         hasher.update(preview_dict[s].encode('utf-8'))
#     return 'preview-' + hasher.hexdigest()

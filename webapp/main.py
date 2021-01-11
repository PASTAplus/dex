#!/usr/bin/env python
import re
import sys
import urllib.parse

sys.path.append("/home/pasta/dex/webapp")
sys.path.append("/home/pasta/dex")

import os
import dex.pasta

os.environ.setdefault("FLASK_ENV", "production")
os.environ.setdefault("FLASK_DEBUG", "0")

import flask
import flask.logging
import logging
import pathlib

import db
import dex.cache
import dex.csv_cache
import dex.exc
import webapp.bokeh_server.views
import webapp.dex.views.eml
import webapp.dex.views.plot
import webapp.dex.views.profile
import webapp.dex.views.subset

# Flask prints the full list of URL query params when in development mode. The DataTable
# widget sends huge queries, so to reduce the noise, we increase the logging level for
# Flask's logger here, and write a brief log record with just the URL in a
# 'before_request' handler.
logging.getLogger("werkzeug").setLevel(logging.WARNING)

root_logger = logging.getLogger("")
root_logger.setLevel(logging.DEBUG)
root_logger.addHandler(flask.logging.default_handler)

log = logging.getLogger(__name__)

app = flask.Flask(
    __name__,
    static_url_path="/static/",
    static_folder=(pathlib.Path(__file__).parent / "static")
    .resolve()
    .as_posix(),
    template_folder=(pathlib.Path(__file__).parent / "templates")
    .resolve()
    .as_posix(),
)

app.config.from_object("webapp.config")
app.debug = app.config["FLASK_DEBUG"]

app.register_blueprint(webapp.bokeh_server.views.bokeh_server)
app.register_blueprint(webapp.dex.views.profile.profile_blueprint)
app.register_blueprint(webapp.dex.views.subset.subset_blueprint)
app.register_blueprint(webapp.dex.views.plot.plot_blueprint)
app.register_blueprint(webapp.dex.views.eml.eml_blueprint)


@app.before_first_request
def register_redirect_handler():
    def handle_redirect_to_index(_):
        return flask.redirect("/", 302)

    app.register_error_handler(
        dex.exc.RedirectToIndex, handle_redirect_to_index
    )


@app.before_request
def before_request():
    log.info(f"{flask.request.method} {flask.request.path}")


@app.after_request
def after_request(response):
    if response.status_code != 200:
        log.error(f"-> {response.status}")
    return response


@app.route("/favicon.ico")
def favicon():
    return flask.send_file(
        app.config["STATIC_PATH"] / "favicon" / "favicon.ico",
        mimetype="image/x-icon",
    )


@app.route("/", methods=["GET"])
def index_get():
    log.info(
        "template root: {}".format(
            (pathlib.Path(__file__).parent / "templates").resolve().as_posix()
        )
    )

    csv_list = sorted(
        [
            {
                'scope_str': scope_str,
                'id_str': id_str,
                'ver_str': ver_str,
                'entity_str': entity_str,
                # 'pkg_id': len(p.suffix),
                # 'pkg_id': p.parent.name + '.' + p.name,
                # 'size': int(123),
                'size': p.stat().st_size,
                # 'status': get_status(p.name),
                'status': '',
            }
            for (p, scope_str, id_str, ver_str, entity_str) in (
                ([p2] + p2.parent.name.split('.') + [p2.name])
                for p2 in app.config['CSV_ROOT_DIR'].glob('**/*')
                if p2.is_file() and len(p2.suffix) == 0
            )
        ],
        key=lambda d: -d['size'],
    )

    return flask.render_template(
        'get_data_url.html', rid=None, entity_tup=None, csv_list=csv_list
    )


@app.route("/sample/<path:data_url>", methods=["GET"])
def sample_get(data_url):
    rid = db.add_entity(data_url)
    return flask.redirect(f'/dex/profile/{rid}')


@app.route("/", methods=["POST"])
def index_post():
    data_url = flask.request.form['data_url']
    rid = db.add_entity(data_url)
    return flask.redirect(f"/dex/profile/{rid}")


if __name__ == "__main__":
    app.run(
        # debug=True,
        # use_debugger=False,
        # use_reloader=False,
        # passthrough_errors=True,
    )

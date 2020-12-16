#!/usr/bin/env python

import sys

sys.path.append("/home/pasta/dex/webapp")
sys.path.append("/home/pasta/dex")

import os

os.environ.setdefault("FLASK_ENV", "production")
os.environ.setdefault("FLASK_DEBUG", "0")

import flask
import flask.logging
import logging
import pathlib

import db
import dex.cache
import dex.csv_cache
import dex.util
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
    static_folder=(pathlib.Path(__file__).parent / "static").resolve().as_posix(),
    template_folder=(pathlib.Path(__file__).parent / "templates").resolve().as_posix(),
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

    app.register_error_handler(dex.util.RedirectToIndex, handle_redirect_to_index)


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


@app.route("/")
def index():
    log.info(
        "template root: {}".format(
            (pathlib.Path(__file__).parent / "templates").resolve().as_posix()
        )
    )
    return flask.render_template(
        "get_data_url.html",
        rid=None,
        entity_tup=None,
    )


@app.route("/<path:csv_url>")
def index2(csv_url):
    # data_url = flask.request.form['csv_url']
    rid = db.add_entity(csv_url)
    log.debug(f"rid={rid}")
    e = rid
    dex.csv_cache.download_full_csv(csv_url, rid)
    return flask.redirect(f"/dex/subset/{rid}")


@app.route("/download", methods=["POST"])
def download():
    data_url = flask.request.form["data_url"]
    rid = db.add_entity(data_url)
    log.debug(f"rid={rid}")
    e = rid
    dex.csv_cache.download_full_csv(data_url, rid)
    return flask.redirect(f"/dex/subset/{rid}")


if __name__ == "__main__":
    app.run(
        # debug=True,
        # use_debugger=False,
        # use_reloader=False,
        # passthrough_errors=True,
    )

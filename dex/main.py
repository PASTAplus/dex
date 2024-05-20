#!/usr/bin/env python

"""DeX entry point
"""
import json
import logging.config
import mimetypes
import os
import pathlib

import flask
import flask.logging
import matplotlib

import dex.cache
import dex.config
import dex.csv_cache
import dex.db
import dex.eml_cache
import dex.exc
import dex.pasta
import dex.sample
import dex.util
import dex.views.api
import dex.views.bokeh_server
import dex.views.eml
import dex.views.plot
import dex.views.profile
import dex.views.subset

matplotlib.use('Agg')

os.environ.setdefault("FLASK_ENV", "production")
os.environ.setdefault("FLASK_DEBUG", "0")


log = logging.getLogger(__name__)

mimetypes.add_type('application/javascript', '.js')


def create_app():
    _app = flask.Flask(
        __name__,
        static_url_path="/static/",
        static_folder=(pathlib.Path(__file__).parent / "static").resolve().as_posix(),
        template_folder=(pathlib.Path(__file__).parent / "templates").resolve().as_posix(),
    )

    _app.config.from_object("dex.config")
    _app.debug = _app.config["FLASK_DEBUG"]

    # Add tojson_pp, a pretty printed version of tojson, to jinja.
    _app.jinja_env.filters['tojson_pp'] = lambda x: json.dumps(
        x, sort_keys=True, indent=4, separators=(', ', ': ')
    )

    _app.register_blueprint(dex.views.bokeh_server.bokeh_server)
    _app.register_blueprint(dex.views.profile.profile_blueprint)
    _app.register_blueprint(dex.views.subset.subset_blueprint)
    _app.register_blueprint(dex.views.plot.plot_blueprint)
    _app.register_blueprint(dex.views.eml.eml_blueprint)
    _app.register_blueprint(dex.views.api.api_blueprint)

    def handle_redirect_to_index(_):
        return flask.redirect("/", 302)

    _app.register_error_handler(dex.exc.RedirectToIndex, handle_redirect_to_index)

    def log_exception(e):
        log.exception('Exception')

    _app.register_error_handler(Exception, log_exception)

    # @_app.before_first_request
    # def before_first_request():
    #     def handle_redirect_to_index(_):
    #         return flask.redirect("/", 302)
    #
    #     _app.register_error_handler(dex.exc.RedirectToIndex, handle_redirect_to_index)

    # def handle_custom_exceptions(e):
    #     log.exception('Exception')
    #     raise
    #
    # _app.register_error_handler(dex.exc.DexError, handle_custom_exceptions)
    # _app.register_error_handler(dex.exc.EMLError, handle_custom_exceptions)
    # _app.register_error_handler(dex.exc.CSVError, handle_custom_exceptions)
    # _app.register_error_handler(dex.exc.CacheError, handle_custom_exceptions)

    # @_app.url_value_preprocessor
    # def url_value_preprocessor(_, q):
    #     if 'toggle-debug' in q:
    #         _app.config['DEBUG_PANEL'] = not _app.config['DEBUG_Panel']
    #         return flask.redirect("/", 302)

    @_app.before_request
    def before_request():
        log.debug(f"{flask.request.method} {flask.request.path}")
        flask.g.debug_panel = flask.request.cookies.get('debug-panel', 'false') == 'true'

    @_app.after_request
    def after_request(response):
        if response.status_code != 200:
            log.debug(f"-> {response.status}")

        if 'debug' in flask.request.args:
            debug_is_enabled = flask.request.args['debug'] == 'true'
            response = flask.make_response(flask.redirect(flask.request.base_url))
            response.set_cookie('debug-panel', str(debug_is_enabled).lower())

        # Add CORS headers to all responses
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
        response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')

        return response

    @_app.route("/favicon.ico")
    def favicon():
        return flask.send_file(
            _app.config["STATIC_PATH"] / "favicon" / "favicon.ico",
            mimetype="image/x-icon",
        )

    @_app.route("/robots.txt")
    def robots_txt():
        """Deny all well behaved web crawlers"""
        return flask.Response("User-agent: *\nDisallow: /", mimetype="text/plain")

    @_app.route("/", methods=["GET"])
    def index_get():
        return flask.render_template(
            'index.html',
            g_dict={},
            csv_list=dex.sample.get_sample_data_entity_list(None),
            # For the base template, should be included in all render_template() calls.
            rid=None,
            data_url=None,
            pkg_id=None,
            csv_name=None,
            portal_base=None,
            note_list=[],
            is_on_pasta=None,
            dbg=None,
        )

    @_app.route("/<path:dist_url>", methods=["GET"])
    def index_get_url(dist_url):
        meta_url = dex.pasta.get_meta_url(dist_url)
        rid = dex.db.add_entity(dist_url, meta_url, dist_url)
        return flask.redirect(f'/dex/profile/{rid}')

    @_app.route("/sample/<path:dist_url>", methods=["GET"])
    def sample_get(dist_url):
        rid = dex.db.add_entity(dist_url, None, None)
        return flask.redirect(f'/dex/profile/{rid}')

    @_app.route("/", methods=["POST"])
    def index_post():
        package_url = flask.request.form['package_url']
        return flask.redirect(f"/{package_url}")

    log.debug('Flask app created')

    return _app


app = create_app()

if __name__ == "__main__":
    app.run(
        # debug=True,
        # use_debugger=False,
        # use_reloader=False,
        # passthrough_errors=True,
    )

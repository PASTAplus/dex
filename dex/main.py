#!/usr/bin/env python

"""Dex entry point
"""
import logging
import logging.config
import mimetypes
import os
import pathlib
import pprint
import random
import sys

import dex.config
import dex.eml_cache

# logging.basicConfig(level=dex.config.LOG_LEVEL)
# pprint.pp(dex.config.LOG_CONFIG)
# logging.config.dictConfig(dex.config.LOG_CONFIG)

import time
import matplotlib

matplotlib.use('Agg')

os.environ.setdefault("FLASK_ENV", "production")
os.environ.setdefault("FLASK_DEBUG", "0")

import flask
import flask.logging

import dex.views.bokeh_server
import dex.db
import dex.cache
import dex.csv_cache
import dex.exc
import dex.pasta
import dex.views.eml
import dex.views.plot
import dex.views.profile
import dex.views.subset
import json

# root_logger = logging.getLogger("")
# root_logger.setLevel(logging.DEBUG)
# root_logger.addHandler(flask.logging.default_handler)

log = logging.getLogger(__name__)

# log.setLevel(logging.DEBUG)

# log.debug('debug')

mimetypes.add_type('application/javascript', '.js')


def create_app():
    print('Setting up logging...', file=sys.stderr)

    logging.basicConfig(
        format='%(name)s %(levelname)-8s %(message)s',
        level=logging.DEBUG,  # if is_debug else logging.INFO,
        stream=sys.stderr,
    )

    log.debug('Creating the Flask app object...')

    _app = flask.Flask(
        __name__,
        static_url_path="/static/",
        static_folder=(pathlib.Path(__file__).parent / "static").resolve().as_posix(),
        template_folder=(pathlib.Path(__file__).parent / "templates").resolve().as_posix(),
    )

    _app.config.from_object("dex.config")
    _app.debug = _app.config["FLASK_DEBUG"]

    logging.getLogger('').setLevel(logging.DEBUG if _app.debug else logging.INFO)

    _app.register_blueprint(dex.views.bokeh_server.bokeh_server)
    _app.register_blueprint(dex.views.profile.profile_blueprint)
    _app.register_blueprint(dex.views.subset.subset_blueprint)
    _app.register_blueprint(dex.views.plot.plot_blueprint)
    _app.register_blueprint(dex.views.eml.eml_blueprint)

    @_app.before_first_request
    def before_first_request():

        import subprocess
        subprocess.run('rm -rf /home/dahl/dev/dex-cache/global', shell=True)


        def handle_redirect_to_index(_):
            return flask.redirect("/", 302)

        _app.register_error_handler(dex.exc.RedirectToIndex, handle_redirect_to_index)

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
    #         _app.config['DEBUG_PANEL'] = not _app.config['DEBUG_PANEL']
    #         return flask.redirect("/", 302)

    @_app.before_request
    def before_request():
        # log.debug(f"{flask.request.method} {flask.request.path}")
        # flask.request.cookies.get('debug-panel', 'false') == 'true'
        flask.g.debug_panel = False

    @_app.after_request
    def after_request(response):
        if response.status_code != 200:
            log.debug(f"-> {response.status}")

        if 'toggle-debug' in flask.request.args:
            is_enabled = flask.request.cookies.get('debug-panel', 'false') == 'true'
            response = flask.make_response(flask.redirect(''))
            response.set_cookie('debug-panel', 'true' if not is_enabled else 'false')
            return response

        return response

    @_app.route("/favicon.ico")
    def favicon():
        return flask.send_file(
            _app.config["STATIC_PATH"] / "favicon" / "favicon.ico",
            mimetype="image/x-icon",
        )

    @_app.route("/", methods=["GET"])
    def index_get():
        log.debug('Rendering index page')
        return flask.render_template(
            'index.html',
            g_dict={},
            rid=None,
            entity_tup=None,
            csv_list=get_sample_data_entity_list(None),
        )

    @_app.route("/<path:data_url>", methods=["GET"])
    def index_get_url(data_url):
        rid = dex.db.add_entity(data_url)
        return flask.redirect(f'/dex/profile/{rid}')

    # @_app.route("/sample/<path:data_url>", methods=["GET"])
    # def sample_get(data_url):
    #     rid = dex.db.add_entity(data_url)
    #     return flask.redirect(f'/dex/profile/{rid}')

    @dex.cache.disk("sample_data_entity_list", "list")
    def get_sample_data_entity_list(_rid, k=200):
        # return []
        entity_list = get_data_entity_list(None)
        if len(entity_list) >= k:
            return sorted(random.sample(entity_list, k), key=lambda d: d['size'])
        return entity_list

    @dex.cache.disk("data_entity_list", "list")
    def get_data_entity_list(_rid):
        data_entity_list = []
        csv_path = _app.config['CSV_ROOT_DIR']
        log.debug(f'Looking for local packages to use as samples at: {csv_path.as_posix()}')

        file_count = 0

        last_ts = time.time()
        for abs_path in csv_path.glob('**/*'):
            abs_path: pathlib.Path
            # rel_path = abs_path.relative_to(csv_path)
            if time.time() - last_ts > 1.0:
                last_ts = time.time()
                log.debug(
                    f'Local CSV discovery: '
                    f'Files checked: {file_count}  Found CSVs: {len(data_entity_list)}'
                )
            if not abs_path.is_file():
                # log.debug('Rejected, not a regular file')
                continue
            file_count += 1
            if not abs_path.stat().st_size >= 10000:
                # log.debug('Rejected, too small to make for an interesting demo')
                continue
            try:
                entity_tup = dex.pasta.get_entity_by_local_path(abs_path)
            except dex.exc.DexError:
                continue
            if not dex.eml_cache.is_local_csv_with_eml(entity_tup):
                # log.debug('Rejected, not a CSV')
                continue
            data_entity_list.append(
                dict(
                    entity=entity_tup,
                    size=abs_path.stat().st_size,
                    abs_path=abs_path,
                )
            )

        log.info(
            f'Local CSV discovery: '
            f'Files checked: {file_count}  Found CSVs: {len(data_entity_list)}'
        )

        return sorted(data_entity_list, key=lambda d: d['size'])

    @_app.route("/sample/<path:data_url>", methods=["GET"])
    def sample_get(data_url):
        rid = dex.db.add_entity(data_url)
        return flask.redirect(f'/dex/profile/{rid}')

    @_app.route("/", methods=["POST"])
    def index_post():
        data_url = flask.request.form['data_url']
        rid = dex.db.add_entity(data_url)
        return flask.redirect(f"/dex/profile/{rid}")

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

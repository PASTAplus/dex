#!/usr/bin/env python

"""DeX entry point
"""
import json
import logging.config
import mimetypes
import os
import pathlib
import random
import time

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
import dex.util
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

    def handle_redirect_to_index(_):
        return flask.redirect("/", 302)

    _app.register_error_handler(dex.exc.RedirectToIndex, handle_redirect_to_index)

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
            csv_list=get_sample_data_entity_list(None),
            # For the base template, should be included in all render_template() calls.
            rid=None,
            entity_tup=None,
            csv_name=None,
            dbg=None,
            portal_base=None,
            note_list=[],
        )

    @_app.route("/<path:data_url>", methods=["GET"])
    def index_get_url(data_url):
        rid = dex.db.add_entity(data_url)
        return flask.redirect(f'/dex/profile/{rid}')

    @_app.route("/<path:package_id>", methods=["DELETE"])
    def flush_cache_for_package(package_id):
        package_deleted = False
        try:
            for rid in dex.db.get_rid_list_by_package_id(package_id):
                log.info(
                    f'Flushing cache files and DB for package. '
                    f'package_id="{package_id}" rid="{rid}"'
                )
                dex.cache.flush_cache(rid)
                dex.db.drop_entity(rid)
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

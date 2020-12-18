"""
"""

import logging
import pathlib
import shlex
import subprocess
import sys
import time

import flask

import db
import webapp.perf
import dex.cache

log = logging.getLogger(__name__)

profile_blueprint = flask.Blueprint(
    "profile", __name__, url_prefix="/dex/profile"
)


@profile_blueprint.route("/<rid>")
def profile(rid):
    if dex.cache.is_cached(rid, "profile", "html"):
        iframe_src = f"/dex/profile/doc/{rid}"
    else:
        iframe_src = f"/dex/profile/generate/{rid}"
    return flask.render_template(
        "profile.html",
        rid=rid,
        entity_tup=db.get_entity_as_dict(rid),
        iframe_src=iframe_src,
    )


@profile_blueprint.route("/generate/<rid>")
def generate(rid):
    return flask.render_template(
        "profile-generate.html", rid=rid, entity_tup=db.get_entity_as_dict(rid)
    )


@profile_blueprint.route("/doc/<rid>")
def doc(rid):
    render_profile(rid)
    return flask.send_file(dex.cache.get_cache_path(rid, "profile", "html"))


@dex.cache.disk("profile", "html")
def render_profile(rid):
    csv_path = dex.cache.get_cache_path(rid, "full", "csv")
    cmd_list = [
        # For debugging in PyCharm, run pandas-profiling in a separate process. The
        # python process is wrapped in a shell script to prevent the debugger from
        # attaching to it. The process crashes if the debugger is able to attach to it.
        # Turning off automatic attach in the PyCharm debugger settings causes the
        # debugger to trigger a crash in Flask.
        flask.current_app.config["PROFILING_SH"].as_posix(),
        flask.current_app.config["PYTHON_BIN"],
        csv_path.resolve().as_posix(),
    ]
    log.debug(
        "Running: {}".format(" ".join([shlex.quote(s) for s in cmd_list]))
    )
    start_ts = time.time()
    html_bytes = subprocess.check_output(cmd_list)
    webapp.perf.set(f"{rid}/profile-sec", time.time() - start_ts)
    log.debug('html_bytes=({}) "{}"'.format(len(html_bytes), html_bytes[:100]))
    return html_bytes.decode("utf-8")
    # cache_path = dex.cache.get_cache_path(
    #     rid, 'profile', 'html', mkdir=True
    # )
    # cache_path.write_bytes(html_bytes)


# @profile_blueprint.route('/profile-fetch/<rid>')
# def profile_render(rid):
#     return flask
#     csv_path = csv_cache.get_csv_path(rid)
#     cmd_list = [
#         # For debugging in PyCharm, run pandas-profiling in a separate process. The
#         # python process is wrapped in a shell script to prevent the debugger from
#         # attaching to it. The process crashes if the debugger is able to attach to it.
#         # Turning off automatic attach in the PyCharm debugger settings causes the
#         # debugger to trigger a crash in Flask.
#         flask.current_app.config['PROFILING_SH'].as_posix(),
#         sys.executable,
#         csv_path.resolve().as_posix(),
#         # sys.executable,
#         # flask.current_app.config['PROFILING_BIN'].as_posix(),
#         # csv_path.resolve().as_posix(),
#     ]
#     log.debug(
#         'Running: {}'.format(' '.join([shlex.quote(s) for s in cmd_list]))
#     )
#     start_ts = time.time()
#     html_bytes = subprocess.check_output(cmd_list)
#     webapp.perf.set(f'{rid}/profile-sec', time.time() - start_ts)
#     log.debug('html_bytes=({}) "{}"'.format(len(html_bytes), html_bytes[:100]))
#     cache_path = flask.current_app.config['CACHE_ROOT_DIR'] / rid
#     cache_path.parent.mkdir(exist_ok=True)
#     cache_path.write_bytes(html_bytes)
#     return flask.send_file(cache_path, mimetype='text/html', )
#
#     # return cache_path
#     # return html_bytes

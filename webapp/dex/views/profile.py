import logging
import shlex
import subprocess
import time

import flask

import db
import perf
import dex.cache
import dex.csv_tmp

log = logging.getLogger(__name__)

profile_blueprint = flask.Blueprint(
    "profile", __name__, url_prefix="/dex/profile"
)


@profile_blueprint.route("/<rid>")
def profile(rid):
    return flask.render_template(
        "profile.html",
        rid=rid,
        entity_tup=db.get_entity_as_dict(rid),
        is_cached=(
            'true' if dex.cache.is_cached(rid, "profile", "html") else 'false'
        ),
    )


@profile_blueprint.route("/doc/<rid>")
def doc(rid):
    if not dex.cache.is_cached(rid, "profile", "html"):
        render_profile(rid)

    def generate():
        with dex.cache.open_file(rid, 'profile', 'html') as f:
            while True:
                b = f.read(flask.current_app.config["CHUNK_SIZE_BYTES"])
                if not b:
                    break
                yield b

        # return flask.send_file(f, mimetype='text/html')
        # return flask.Response(flask.stream_with_context(f), status=200, mimetype='text/html')

    return flask.Response(
        flask.stream_with_context(generate()), status=200, mimetype='text/html'
    )


@dex.cache.disk("profile", "html")
def render_profile(rid):
    csv_path = dex.csv_tmp.get_data_path_by_row_id(rid)
    # csv_path = dex.cache.get_cache_path(rid, "full", "csv")

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
    perf.set(f"{rid}/profile-sec", time.time() - start_ts)
    log.debug('html_bytes=({}) "{}"'.format(len(html_bytes), html_bytes[:100]))

    # cache_path = dex.cache.get_cache_path(
    #     rid, 'profile', 'html', mkdir=True
    # )
    # cache_path.write_bytes(html_bytes)

    return html_bytes.decode("utf-8")

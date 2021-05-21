import json
import logging
import pathlib
import shlex
import subprocess
import tempfile

import flask
import flask.json
from flask import current_app as app

import db
import dex.cache
import dex.csv_cache
import dex.csv_parser
import dex.csv_tmp
import dex.debug
import dex.eml_cache

log = logging.getLogger(__name__)

profile_blueprint = flask.Blueprint("profile", __name__, url_prefix="/dex/profile")


@profile_blueprint.route("/<rid>")
def profile(rid):
    return flask.render_template(
        "profile.html",
        g_dict=dict(
            is_cached=dex.cache.is_cached(rid, "profile", "html"),
        ),
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        entity_tup=db.get_entity_as_dict(rid),
        dbg=dex.debug.debug(rid),
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
    # csv_path = dex.csv_tmp.get_data_path_by_row_id(rid).resolve().as_posix()
    webapp_path = pathlib.Path(__file__).resolve().parents[2]

    py_interpreter_path = app.config["PYTHON_BIN"]
    py_proc_path = (webapp_path / 'profiling_proc.py').as_posix()
    sh_proc_path = (webapp_path / 'profiling_proc.sh').as_posix()
    yml_config_path = (webapp_path / 'profiling_config.yml').as_posix()

    with tempfile.NamedTemporaryFile(
        mode='wt',
        encoding='utf-8',
        prefix=f'profile_args_{rid}',
        suffix='.json',
        delete=False,
    ) as f:
        flask.json.dump(
            dict(
                # For profiling_proc.sh:
                py_interpreter_path=py_interpreter_path,
                py_proc_path=py_proc_path,
                sh_proc_path=sh_proc_path,
                # For profiling_proc.py:
                rid=rid,
                yml_config_path=yml_config_path,
                dark_mode=True,
            ),
            f,
        )
        cmd_list = [sh_proc_path, py_interpreter_path, py_proc_path, f.name]
        log.debug("Running: {}".format(" ".join([shlex.quote(s) for s in cmd_list])))
        # start_ts = time.time()
    try:
        html_bytes = subprocess.check_output(cmd_list)
    except subprocess.CalledProcessError as e:
        raise Exception(f'stdout="{e.stdout}" stderr="{e.stderr}"')

    return html_bytes.decode("utf-8")

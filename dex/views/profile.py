import logging

import flask
import flask.json
from flask import current_app as app

import pandas_profiling

import dex.db
import dex.cache
import dex.csv_cache
import dex.csv_parser
import dex.obj_bytes
import dex.debug
import dex.eml_cache
import dex.util

log = logging.getLogger(__name__)

profile_blueprint = flask.Blueprint("profile", __name__, url_prefix="/dex/profile")


# noinspection PyUnresolvedReferences
@profile_blueprint.route("/<rid>")
def profile(rid):
    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)

    note_list = []
    if len(csv_df) == app.config['CSV_MAX_CELLS'] // len(eml_ctx['column_list']):
       note_list.append('Due to size, only the first part of this table is available in DeX')

    note_list.append('This analysis may not match the EML metadata for all columns')

    return flask.render_template(
        "profile.html",
        g_dict=dict(
            is_cached=dex.cache.is_cached(rid, "profile", "html"),
            rid=rid,
        ),
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        dbg=dex.debug.debug(rid),
        portal_base=dex.pasta.get_portal_base_by_entity(dex.db.get_entity(rid)),
        note_list=note_list,
    )


@profile_blueprint.route("/doc/<rid>")
def doc(rid):
    """Return the Pandas Profiling HTML doc for the given rid. If the profile has not
    been generated, this holds the connection until the doc is ready, then returns it."""
    # if not dex.cache.is_cached(rid, "profile", "html"):
    render_profile(rid)

    def chunks_gen():
        with dex.cache.open_file(rid, 'profile', 'html') as f:
            while True:
                b = f.read(flask.current_app.config["CHUNK_SIZE_BYTES"])
                if not b:
                    break
                yield b

        # return flask.send_file(f, mimetype='text/html')
        # return flask.Response(flask.stream_with_context(f), status=200, mimetype='text/html')

    return flask.Response(flask.stream_with_context(chunks_gen()), status=200, mimetype='text/html')


@dex.cache.disk("profile", "html")
def render_profile(rid):
    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)

    log.debug('Calling pandas_profiling.ProfileReport()...')

    # Create a tree representation of the report.
    report_tree = pandas_profiling.ProfileReport(
        csv_df,
        config_file=flask.current_app.config['PROFILING_CONFIG_PATH'],
        # dark_mode=arg_dict['dark_mode'],
        infer_dtypes=False,
        # correlation_overrides=csv_df.columns.tolist(),
        # correlation_threshold=1,
        # check_correlation=False,
        correlations={
            # "pearson": False,
            # "spearman": False,
            # "kendall": False,
            # "phi_k": False,
            # "cramers": False,
            # "recoded": False,
        },
        # plot={'histogram': {'bins': None}},
    )
    rearrange_report(report_tree)
    html_str = report_tree.to_html()
    return html_str


def rearrange_report(report_tree):
    try:
        # Move the Sample section from the end to the front of the report.
        section_list = report_tree.report.content["body"].content["items"]
        try:
            section_list.insert(1, section_list.pop(-1))
        except Exception:
            log.exception('Unable to reorder the sections of the Pandas Profiling report')

        items = section_list[0].content['items']
        items[1].content['name'] = 'Notes'
        items[2].content['name'] = 'Report Reproducibility'
        items[2].content['items'][0].content['name'] = 'Report Reproducibility'
    except Exception:
        log.exception('Unable to rename parts of the Pandas Profiling report')

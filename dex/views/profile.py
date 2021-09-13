import logging

import flask
import flask.json
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
    return flask.render_template(
        "profile.html",
        g_dict=dict(
            is_cached=dex.cache.is_cached(rid, "profile", "html"),
        ),
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        dbg=dex.debug.debug(rid),
    )


@profile_blueprint.route("/doc/<rid>")
def doc(rid):
    """Return the Pandas Profiling HTML doc for the given rid. If the profile has not
    been generated, this holds the connection until the doc is ready, then returns it."""
    if not dex.cache.is_cached(rid, "profile", "html"):
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
    ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    csv_df = ctx['csv_df']

    for i, col_name in enumerate(csv_df.columns):
        # TODO: We have similar logic in cast_to_eml_types(). Check if they can be merged.
        derived_dict = ctx['derived_dtypes_list'][i]
        if   derived_dict['type_str'] == 'TYPE_CAT' or derived_dict['is_enumerated']:
            csv_df[col_name] = csv_df[col_name].astype('category', errors='ignore')
        elif derived_dict['type_str'] == 'TYPE_NUM':
            csv_df[col_name] = csv_df[col_name].astype(np.float, errors='ignore')
        elif derived_dict['c_date_fmt_str']: # derived_dict['type_str'] == 'TYPE_DATE' and
            csv_df[col_name] = pd.to_datetime(csv_df[col_name], errors='ignore', format=derived_dict['c_date_fmt_str'])

    log.debug('Calling pandas_profiling.ProfileReport()...')

    # Create a tree representation of the report.
    report_tree = pandas_profiling.ProfileReport(
        csv_df,
        config_file=flask.current_app.config['PROFILING_CONFIG_PATH'],
        # dark_mode=arg_dict['dark_mode'],
        infer_dtypes=False,
    )

    # rearrange_report(report_tree)
    # dex.util.dump_full_dataframe(csv_df)
    html_str = report_tree.to_html()
    return html_str


def rearrange_report(report_tree):
    # Move the Sample section from the end to the front of the report.
    section_list = report_tree.report.content["body"].content["items"]
    section_list.insert(1, section_list.pop(-1))

    try:
        section_list[0].content['items'][1].content['name'] = 'Notes'
        section_list[0].content['items'][2].content['name'] = 'Reproducibility'
        section_list[0].content['items'][2].content['items'][0].content['name'] = 'Reproducibility'
    except Exception:
        log.exception('Unable to reorder the sections of the Pandas Profiling report')

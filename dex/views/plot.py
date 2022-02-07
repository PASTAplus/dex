import contextlib
import logging

import flask

import dex.db
import dex.csv_cache
import dex.csv_parser
import dex.debug
import dex.eml_cache
import dex.eml_types
import dex.util
import dex.pasta
import pandas.api.types

from flask import current_app as app

log = logging.getLogger(__name__)

plot_blueprint = flask.Blueprint("plot", __name__, url_prefix="/dex/plot")


@plot_blueprint.route("/<rid>")
def plot(rid):
    """DATE is only on X
    NUM is on X and Y
    CAT is not on either axis
    UNSUPPORTED is not on either axis
    """
    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)

    col_list = []

    col_agg_dict = dex.csv_cache.get_col_agg_dict(csv_df, eml_ctx)

    for col_idx, col_dict in enumerate(eml_ctx['column_list']):

        if col_idx not in col_agg_dict:
            continue

        agg_dict = col_agg_dict[col_idx]

        d = N(**col_dict)

        friendly_type = dex.eml_types.PANDAS_TO_FRIENDLY_DICT[d.pandas_type]
        sel_str = '{} ({}, {} - {})'.format(
            d.col_name,
            friendly_type,
            agg_dict['v_min'],
            agg_dict['v_max'],
        )
        col_list.append((col_idx, sel_str))

    g_dict = dict(
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        col_list=col_list,
        # cols_y=sel_list_y,
        y_not_applied_str='Not applied',
    )

    dex.util.logpp(g_dict, msg='Plot g_dict', logger=log.debug)

    rows_total = len(csv_df)
    rows_used = app.config["CSV_SAMPLE_THRESHOLD"]

    note_list = []
    if rows_total == app.config['CSV_MAX_ROWS']:
       note_list.append('Due to size, only the first part of this table is available in DeX')
    # if rows_used < rows_total:
    if True:
        note_list.append('The data for this plot are subsampled')

    return flask.render_template(
        "plot.html",
        g_dict=g_dict,
        col_list=col_list,
        # cols_x=col_list,
        # cols_y=sel_list_y,
        rows_total=rows_total,
        rows_used=rows_used,
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        dbg=dex.debug.debug(rid),
        portal_base=dex.pasta.get_portal_base_by_entity(dex.db.get_entity(rid)),
        note_list=note_list,
    )


@plot_blueprint.route("/test")
def test():
    return flask.render_template(
        "test.html",
        g_dict={
            'options': [
                (10, 'option 10'),
                (20, 'option 20'),
                (30, 'option 30'),
                (40, 'option 40'),
                (50, 'option 50'),
            ],
            'sel': [
                20,
                40,
            ],
        },
    )

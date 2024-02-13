import json
import logging

import flask
from flask import current_app as app

import dex.csv_cache
import dex.csv_parser
import dex.db
import dex.debug
import dex.eml_cache
import dex.eml_date_fmt
import dex.eml_extract
import dex.pasta
import dex.util
import dex.views.util

log = logging.getLogger(__name__)

plot_blueprint = flask.Blueprint("plot", __name__, url_prefix="/dex/plot")


@plot_blueprint.route("/<rid>", methods=['GET', 'POST'])
def plot(rid):
    """DATE is only on X
    NUM is on X and Y
    CAT is not on either axis
    UNSUPPORTED is not on either axis
    """
    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)
    full_row_count = subset_row_count = len(csv_df)

    # Plot a subset
    subset_dict = None
    subset_json = flask.request.form.get('subset')
    if subset_json:
        subset_dict = json.loads(subset_json)
        if subset_dict is not None:
            csv_df = dex.views.util.create_subset(rid, csv_df, subset_dict)
            subset_row_count = len(csv_df)

    #
    col_list = []
    col_agg_dict = dex.csv_cache.get_plottable_col_aggregates(csv_df, eml_ctx)

    for col_idx, col_dict in enumerate(eml_ctx['column_list']):
        if col_idx not in col_agg_dict:
            continue

        agg_dict = col_agg_dict[col_idx]

        d = N(**col_dict)

        friendly_type = dex.eml_extract.PANDAS_TO_FRIENDLY_DICT[d.pandas_type]
        sel_str = '{} ({}, {} - {})'.format(
            d.col_name,
            friendly_type,
            agg_dict['v_min'],
            agg_dict['v_max'],
        )
        col_list.append((col_idx, sel_str))

    # dex.util.logpp(g_dict, msg='Plot g_dict', logger=log.debug)

    note_list = []
    if full_row_count == app.config['CSV_MAX_CELLS'] // len(eml_ctx['column_list']):
        note_list.append('Due to size, only the first part of this table is available in DeX')
    if full_row_count > subset_row_count:
        note_list.append(
            f'Plotting a subset containing {subset_row_count} of {full_row_count} rows'
        )

    g_dict = dict(
        rid=rid,
        pkg_id=dex.eml_cache.get_pkg_id_dict(rid),
        col_list=col_list,
        y_not_applied_str='Not applied',
        subset_dict=subset_dict,
    )
    return flask.render_template(
        "plot.html",
        g_dict=g_dict,
        col_list=col_list,
        # cols_x=col_list,
        # cols_y=sel_list_y,
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        data_url=dex.db.get_data_url(rid),
        pkg_id=dex.eml_cache.get_pkg_id_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        portal_base=dex.pasta.get_portal_base(dex.db.get_dist_url(rid)),
        note_list=note_list,
        is_on_pasta=dex.pasta.is_on_pasta(dex.db.get_meta_url(rid)),
        dbg=dex.debug.debug(rid),
    )

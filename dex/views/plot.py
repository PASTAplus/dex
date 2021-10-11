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

    sel_list_x = []
    sel_list_y = []

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
        if d.pandas_type == dex.eml_types.PandasType.DATETIME:
            sel_list_x.append((sel_str, col_idx))
        elif dex.eml_types.is_numeric(d.pandas_type):
            sel_list_x.append((sel_str, col_idx))
            sel_list_y.append((sel_str, col_idx))

    g_dict = dict(
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        cols_x=sel_list_x,
        cols_y=sel_list_y,
    )

    dex.util.logpp(g_dict, msg='Plot g_dict', logger=log.debug)

    return flask.render_template(
        "plot.html",
        g_dict=g_dict,
        cols_x=sel_list_x,
        cols_y=sel_list_y,
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        entity_tup=dex.db.get_entity_as_dict(rid),
        csv_name=dex.eml_cache.get_csv_name(rid),
        dbg=dex.debug.debug(rid),
        portal_base=dex.pasta.get_portal_base_by_entity(dex.db.get_entity(rid)),
    )

import logging

import flask

import db
import dex.csv_cache
import dex.csv_parser
import dex.debug
import dex.eml_cache
import util

log = logging.getLogger(__name__)

plot_blueprint = flask.Blueprint("plot", __name__, url_prefix="/dex/plot")


@plot_blueprint.route("/<rid>")
def plot(rid):
    """DATE is only on X
    NUM is on X and Y
    CAT is not on either axis
    UNSUPPORTED is not on either axis
    """
    ctx = dex.csv_parser.get_parsed_csv_with_context(rid)

    sel_list_x = []
    sel_list_y = []

    for col_idx, dtype_dict in enumerate(ctx['derived_dtypes_list']):
        d = N(**dtype_dict)

        agg_dict = ctx['col_agg_dict'].get(col_idx)
        if agg_dict is None:
            continue

        friendly_type = dex.csv_parser.DTYPE_TO_FRIENDLY_DICT[d.type_str]
        # agg_dict = ctx['col_agg_dict'][col_idx]
        sel_str = '{} ({}, {} - {})'.format(
            d.col_name, friendly_type, agg_dict['v_min'], agg_dict['v_max']
        )
        if d.type_str == 'TYPE_DATE':
            sel_list_x.append((sel_str, col_idx))
        elif d.type_str == 'TYPE_NUM':
            sel_list_x.append((sel_str, col_idx))
            sel_list_y.append((sel_str, col_idx))

    g_dict = dict(
        rid=rid,
        entity_tup=db.get_entity_as_dict(rid),
        cols_x=sel_list_x,
        cols_y=sel_list_y,
    )

    util.logpp(g_dict, msg='Plot g_dict', logger=log.debug)

    return flask.render_template(
        "plot.html",
        g_dict=g_dict,
        cols_x=sel_list_x,
        cols_y=sel_list_y,
        # For the base template, should be included in all render_template() calls.
        rid=rid,
        entity_tup=db.get_entity_as_dict(rid),
        dbg=dex.debug.debug(rid),
    )

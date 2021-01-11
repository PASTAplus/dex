import logging

import flask
import numpy
import pandas

import db
import dex.csv_cache
import dex.eml_cache

log = logging.getLogger(__name__)

plot_blueprint = flask.Blueprint("plot", __name__, url_prefix="/dex/plot")


@plot_blueprint.route("/<rid>")
def profile(rid):
    # rid = db.get_entity(rid)

    # csv_df = csv_cache.get_full_csv(rid)
    desc_df = dex.csv_cache.get_description(rid)

    pandas.set_option(
        "display.max_rows",
        100,
        "display.max_columns",
        None,
        "display.max_colwidth",
        10000,
        "display.width",
        10000,
    )

    dt_col_dict = dex.eml_cache.get_datetime_columns_as_dict(rid)
    dt_col_set = set(dt_col_dict.keys())

    sel_list_x = []
    sel_list_y = []

    for i, col_name in enumerate(desc_df.columns):
        c = desc_df.iloc[:, i]
        if i in dt_col_set:
            sel_str = f'{col_name} (Time Series, {dt_col_dict[i]["begin_dt"]} - {dt_col_dict[i]["end_dt"]})'
            sel_list_x.append((sel_str, i))
        elif c.dtype == numpy.number:
            sel_str = f'{col_name} (min={c["min"]:,.02f} max={c["max"]:,.02f})'
            sel_list_x.append((sel_str, i))
            sel_list_y.append((sel_str, i))
        else:
            # Skip categorical data. TODO: Do we want to include categories if they're numerical, and so can be plotted?
            pass
            # sel_str = f'{col_name} (Categorical)'
            # sel_list_x.append((sel_str, i))
            # sel_list_y.append((sel_str, i))

    return flask.render_template(
        "plot.html",
        rid=rid,
        entity_tup=db.get_entity_as_dict(rid),
        cols_x=sel_list_x,
        cols_y=sel_list_y,
    )

    # unique_list = sorted(csv_df[col_name].unique().tolist())
    # if (
    #     len(unique_list) / len(csv_df)
    #     > (flask.current_app.config['CATEGORY_THRESHOLD_PERCENT'] / 100)
    # ):
    #     return json.dumps([])
    # return json.dumps(unique_list)

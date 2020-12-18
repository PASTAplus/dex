import json
import pathlib
import time
import dex.eml_cache

import bokeh.colors.rgb
import bokeh.embed

from bokeh.plotting import figure

import flask
import pandas as pd

import dex.csv_cache
import dex.cache

bokeh_server = flask.Blueprint("bokeh", "bokeh", url_prefix="/bokeh")

THEME_DICT = {
    "light": {"fg_color": "black",},
    "dark": {"fg_color": "#a59a89",},
    "default": {"fg_color": "white",},
}


@bokeh_server.route("/col-plot/<rid>/<col>")
def col_plot(rid, col):
    theme_key = flask.request.args.get("theme", "default")
    fg_color = THEME_DICT[theme_key]["fg_color"]

    plot_json = get_plot_from_cache(rid, col, theme_key)

    if plot_json:
        return plot_json

    d = dex.csv_cache.get_col_name_by_index(rid, int(col))

    # output to static HTML file
    # output_file('lines.html')

    # create a new plot with a title and axis labels
    fig = figure(plot_width=400, plot_height=35)

    fig.axis.visible = False
    fig.toolbar.logo = None
    fig.toolbar_location = None
    fig.xgrid.visible = False
    fig.ygrid.visible = False
    fig.outline_line_color = None
    fig.background_fill_color = None
    fig.border_fill_color = None

    fig.x_range.range_padding = 0

    fig.margin = 0
    fig.min_border = 0
    # fig.min_border_top = 5
    # fig.min_border_bottom = 5

    # fig.min_border_left = 0
    # fig.min_border_right = 0
    # fig.min_border_top = 0
    # fig.min_border_bottom = 0

    # Disable user interaction
    fig.toolbar.active_drag = None
    fig.toolbar.active_scroll = None
    fig.toolbar.active_tap = None

    # add a line renderer with legend and line thickness
    # fig.line(
    #     range(len(d)), d, line_width=2, color=bokeh.colors.RGB(167, 158, 139)
    # )
    # fig.dot(range(len(d)), d, size=5, color=bokeh.colors.RGB(167, 158, 139))
    fig.dot(range(len(d)), d, size=5, color=fg_color)
    plot_json = json.dumps(bokeh.embed.json_item(fig))

    add_plot_to_cache(rid, col, plot_json, theme_key)
    return plot_json


def get_plot_from_cache(rid, col, fg_color):
    p = get_plot_cache_path(rid, col, fg_color)
    if p.exists():
        return p.read_text()


def add_plot_to_cache(rid, col, plot_json, fg_color):
    p = get_plot_cache_path(rid, col, fg_color)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(plot_json)


def get_plot_cache_path(rid, col, fg_color):
    return dex.cache.get_cache_path(rid, f"{col}-{fg_color}", "plot")


@bokeh_server.route("/xy-plot/<rid>/<x_col_idx>/<y_col_idx>")
def xy_plot(rid, x_col_idx, y_col_idx):
    theme_key = flask.request.args.get("theme", "default")
    fg_color = THEME_DICT[theme_key]["fg_color"]

    df = dex.csv_cache.get_csv_sample(rid)

    x_col_idx = int(x_col_idx)
    y_col_idx = int(y_col_idx)
    x = df.iloc[:, int(x_col_idx)]
    y = df.iloc[:, int(y_col_idx)]
    # x = dex.csv_cache.get_column_iloc(rid, int(x))
    # y = dex.csv_cache.get_column_iloc(rid, int(y))

    datetime_col_list = dex.eml_cache.get_datetime_columns(rid)
    is_dt = x_col_idx in [d["col_idx"] for d in datetime_col_list]

    fig = figure(
        plot_width=500,
        plot_height=500,
        x_axis_label=df.columns[x_col_idx],
        y_axis_label=df.columns[y_col_idx],
        x_axis_type="datetime" if is_dt else "auto",
    )
    if is_dt:
        x_list = dex.csv_cache.get_dt_col(rid, x_col_idx)
    else:
        x_list = x.fillna("interpolate").to_list()

    y_list = y.fillna("interpolate").to_list()

    # print(x_list)
    # print(y_list)
    fig.dot(x_list, y_list, size=20, color="#000000")
    # fig.line(x.to_list(), y.to_list(),  color='#000000')
    plot_json = json.dumps(bokeh.embed.json_item(fig))

    # Test behavior for slow plot
    # time.sleep(10)

    return plot_json

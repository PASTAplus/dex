import json
import logging

import bokeh.colors.rgb
import bokeh.embed
import bokeh.models
import bokeh.models.markers
import bokeh.palettes
import bokeh.plotting
import flask

import dex.cache
import dex.csv_cache
import dex.eml_cache
import dex.util

log = logging.getLogger(__name__)

bokeh_server = flask.Blueprint("bokeh", "bokeh", url_prefix="/bokeh")

THEME_DICT = {
    "light": {"fg_color": "black"},
    "dark": {"fg_color": "#a59a89"},
    "default": {"fg_color": "white"},
}


# TODO: Check if these functions can use the regular disk caching now.


@bokeh_server.route("/col-plot/<rid>/<col>")
def col_plot(rid, col):
    """Create a plot of all the values in a single column."""
    theme_key = flask.request.args.get("theme", "default")
    fg_color = THEME_DICT[theme_key]["fg_color"]

    plot_json = get_plot_from_cache(rid, col, theme_key)

    if plot_json:
        return plot_json

    # output to static HTML file
    # output_file('lines.html')

    # create a new plot with a title and axis labels
    fig = bokeh.plotting.figure(plot_width=400, plot_height=35)

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
    d = dex.csv_cache.get_col_name_by_index(rid, int(col))
    fig.dot(range(len(d)), d, size=5, color=fg_color)
    plot_json = json.dumps(
        bokeh.embed.json_item(fig),
        cls=dex.util.DatetimeEncoder,
    )

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

@bokeh_server.route("/xy-plot/<rid>/<parm_uri>")
def xy_plot(rid, parm_uri):
    # parm_dict = N(**json.loads(parm_uri))
    parm_dict = json.loads(parm_uri)
    log.debug(f'parm_dict="{parm_dict}"')

    theme_key = flask.request.args.get("theme", "default")
    fg_color = THEME_DICT[theme_key]["fg_color"]

    df = dex.csv_cache.get_csv_sample(rid)

    # df.sort_values('TIMESTAMP', inplace=True)

    x_col_idx = parm_dict['x']
    x = df.iloc[:, x_col_idx]

    datetime_col_list = dex.eml_cache.get_datetime_columns(rid)
    is_dt = x_col_idx in [d["col_idx"] for d in datetime_col_list]

    # The figure is the container for the whole plot.
    fig = bokeh.plotting.figure(
        plot_width=500,
        plot_height=500,
        x_axis_label=df.columns[x_col_idx],
        # y_axis_label=df.columns[y_col_idx],
        x_axis_type="datetime" if is_dt else "auto",
        # legend_label='Y1',
        title=dex.eml_cache.get_csv_name(rid),
        tooltips=[
            ("Row", "$index"),
            ("(x,y)", "($x, $y)"),
        ]
    )

    # color_list = bokeh.palettes.inferno(len(parm_dict['y']))
    color_list = bokeh.palettes.turbo(len(parm_dict['y']))
    source = bokeh.models.ColumnDataSource(df)

    # Glyphs are individual plot elements.
    glyph_list = []

    for y_idx, (y_col_idx, draw_lines_bool) in enumerate(parm_dict['y']):
        color_str = color_list[y_idx]

        m = list(bokeh.models.markers.marker_types.keys())

        # All the markers in a scatter plot is a single glyph
        glyph = bokeh.models.Scatter(x=df.columns[x_col_idx], y=df.columns[y_col_idx],
                                     size=7, fill_color=color_str, marker=m[y_idx])
        glyph_renderer = fig.add_glyph(source, glyph)
        legend_list = [glyph_renderer]

        if draw_lines_bool:
            # All the line segments in a plot is a single glyph
            glyph = bokeh.models.Line(x=df.columns[x_col_idx], y=df.columns[y_col_idx],
                                      line_color=color_str)
            glyph_renderer = fig.add_glyph(source, glyph)
            legend_list.append(glyph_renderer)

        glyph_list.append((df.columns[y_col_idx], legend_list))

        # fig.line(x_sorted_list, y_sorted_list, color=color_str)
        # bokeh.models.LegendItem()

    legend = bokeh.models.Legend(items=glyph_list)
    legend.click_policy = "hide"

    # Place legend inside the grid
    legend.location = "top_right"

    # Place legend outside the grid
    # fig.add_layout(legend, 'right')

    fig.add_layout(legend)

    plot_json = json.dumps(bokeh.embed.json_item(fig), cls=dex.util.DatetimeEncoder)

    # Simulate large obj/slow server
    # import time
    # time.sleep(5)

    return plot_json

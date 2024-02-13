import json
import logging

import bokeh.colors
import bokeh.core.enums
import bokeh.embed
import bokeh.models
import bokeh.palettes
import bokeh.plotting
import flask

import dex.cache
import dex.csv_cache
import dex.csv_parser
import dex.eml_cache
import dex.util
import dex.views.util

log = logging.getLogger(__name__)

bokeh_server = flask.Blueprint("bokeh", "bokeh", url_prefix="/bokeh")

THEME_DICT = {
    "light": {"fg_color": "black"},
    "dark": {"fg_color": "#a59a89"},
    "default": {"fg_color": "white"},
}

MARKER_TYPE_TUP = tuple(bokeh.core.enums.MarkerType)

# TODO: Check if these functions can use the regular disk caching now.


@bokeh_server.route("/xy-plot/<rid>/<width>/<parm_uri>")
def xy_plot(rid, width, parm_uri):
    # parm_dict = N(**json.loads(parm_uri))
    parm_dict = json.loads(parm_uri)
    log.debug(f'parm_dict="{parm_dict}"')

    # theme_key = flask.request.args.get("theme", "default")
    # fg_color = THEME_DICT[theme_key]["fg_color"]

    csv_df, raw_df, eml_ctx = dex.csv_parser.get_parsed_csv_with_context(rid)

    # If a subset was included in the query args, only plot the subset
    subset_json = flask.request.args.get('subset')
    if subset_json:
        subset_dict = json.loads(subset_json)
        if subset_dict is not None:
            csv_df = dex.views.util.create_subset(rid, csv_df, subset_dict)

    # If there are still too many points to plot (after possible subset), subsample
    # the plot.
    csv_df = dex.csv_cache.get_sample(csv_df)

    # When the lines function is used, it's important to plot the points in the correct
    # order (to avoid criss-crossing lines).
    csv_df = csv_df.sort_index()

    # csv_df.sort_values('TIMESTAMP', inplace=True)

    x_col_idx = parm_dict['x']
    x = csv_df.iloc[:, x_col_idx]

    datetime_col_list = dex.eml_cache.get_datetime_columns(rid)
    is_dt = x_col_idx in [d["col_idx"] for d in datetime_col_list]

    # The figure is the container for the whole plot.
    fig = bokeh.plotting.figure(
        width=int(width),
        height=800,
        x_axis_label=csv_df.columns[x_col_idx],
        # y_axis_label=csv_df.columns[y_col_idx],
        x_axis_type="datetime" if is_dt else "auto",
        # legend_label='Y1',
        title=dex.eml_cache.get_csv_name(rid),
        tooltips=[
            ("Row", "$index"),
            ("(x,y)", "($x, $y)"),
        ],
    )

    # color_list = bokeh.palettes.inferno(len(parm_dict['y']))
    color_list = bokeh.palettes.turbo(len(parm_dict['y']))
    source = bokeh.models.ColumnDataSource(csv_df)

    # Glyphs are individual plot elements.
    glyph_list = []

    for y_idx, (y_col_idx, draw_lines_bool) in enumerate(parm_dict['y']):
        color_str = color_list[y_idx]

        # All the markers in a scatter plot is a single glyph
        glyph = bokeh.models.Scatter(
            x=csv_df.columns[x_col_idx],
            y=csv_df.columns[y_col_idx],
            size=7,
            fill_color=color_str,
            marker=MARKER_TYPE_TUP[y_idx],
        )
        glyph_renderer = fig.add_glyph(source, glyph)
        legend_list = [glyph_renderer]

        if draw_lines_bool:
            # All the line segments in a plot is a single glyph
            glyph = bokeh.models.Line(
                x=csv_df.columns[x_col_idx], y=csv_df.columns[y_col_idx], line_color=color_str
            )
            glyph_renderer = fig.add_glyph(source, glyph)
            legend_list.append(glyph_renderer)

        glyph_list.append((csv_df.columns[y_col_idx], legend_list))

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

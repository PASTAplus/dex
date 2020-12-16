#!/usr/bin/env python

from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, output_file, show
import pandas

d = [6, 7, 2, 4, 5]

# Output to static HTML file
output_file("lines.html")

# Create a new plot with a title and axis labels
p = figure(plot_width=300, plot_height=50)

p.axis.visible = False
p.toolbar.logo = None
p.toolbar_location = None
p.xgrid.visible = False
p.ygrid.visible = False
p.outline_line_color = None

# Add a line renderer with legend and line thickness
p.line(range(len(d)), d, line_width=2)

# Show the results
show(p)

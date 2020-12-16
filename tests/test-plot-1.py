#!/usr/bin/env python

from bokeh.models import ColumnDataSource
from bokeh.plotting import figure, output_file, show
import pandas

data_frame = pandas.read_csv("~/dev/dex/csv/0d22a71b419b73bebe5f62ffff2131fb")

source = ColumnDataSource(data=data_frame)

# has new, identical-length updates for all columns in source
# new_data = {
#     'foo': [10, 20],
#     'bar': [100, 200],
# }

# source.stream(new_data)

# prepare some data
x = [1, 2, 3, 4, 5]
y = [6, 7, 2, 4, 5]

# Output to static HTML file
output_file("lines.html")

# Create a new plot with a title and axis labels
p = figure(title="simple line example", x_axis_label="x", y_axis_label="y")

# Add a line renderer with legend and line thickness
p.line(x, y, legend_label="Temp.", line_width=2)

# Show the results
show(p)

#!/usr/bin/env python

import pandas as pd
import sys
import pandas_profiling

csv_path = sys.argv[1]
config_path = sys.argv[2]

df = pd.read_csv(csv_path)

# Create a tree representation of the report.
report_tree = pandas_profiling.ProfileReport(df, config_file=config_path, dark_mode=True)

# Move the Sample section from the end to the front of the report.
section_list = report_tree.report.content["body"].content["items"]
section_list.insert(1, section_list.pop(-1))

section_list[0]\
    .content['items'][1]\
    .content['name'] = 'Notes'

section_list[0]\
    .content['items'][2]\
    .content['name'] = 'Reproducibility'

section_list[0]\
    .content['items'][2]\
    .content['items'][0]\
    .content['name'] = 'Reproducibility'

# Render the tree to HTML
html_str = report_tree.to_html()

# Return the profile report HTML doc to caller via stdout
print(html_str)

#!/usr/bin/env bash

# Wrap the profiling Python script to prevent the PyCharm debugger from attempting to
# attach, which causes the debugger to crash.
#
# The resulting HTML doc is transferred from the profiling script to the Flask app via
# stdout-stdin, while errors and any other output that is required must written to
# stderr, or it will be break the profile HTML doc.

PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin"

py_interpreter_path="$1"
py_proc_path="$2"
json_config_path="$3"
log_path="${json_config_path}.log"

printf >&2 'py_interpreter_path: %s\n' "$py_interpreter_path"
printf >&2 'py_proc_path: %s\n' "$py_proc_path"
printf >&2 'json_config_path: %s\n' "$json_config_path"
printf >&2 'log_path: %s\n' "$log_path"

"$py_interpreter_path" "$py_proc_path" "$json_config_path"
# 2> "${log_path}"

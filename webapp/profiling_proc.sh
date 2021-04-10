#!/usr/bin/env bash

# Wrap the profiling Python script to prevent the PyCharm debugger from attempting to attach, which causes the debugger to crash.

PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin"

py_interpreter_path="$1"
py_proc_path="$2"
json_config_path="$3"

#printf 'py_interpreter_path: %s\n' "$py_interpreter_path"
#printf 'py_proc_path: %s\n' "$py_proc_path"
#printf 'json_config_path: %s\n' "$json_config_path"

"$py_interpreter_path" "$py_proc_path" "$json_config_path"

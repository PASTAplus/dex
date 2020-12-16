#!/bin/bash

# Wrap the profiling Python script to prevent the PyCharm debugger from attempting to attach, which causes the debugger to crashes.

PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/snap/bin

py_bin=$1
csv_path=$2
this_dir="$(dirname "$(readlink -f "$0")")"

#echo "py_bin: ${py_bin}"
#echo "csv_path: ${csv_path}"
#echo "this_dir: ${this_dir}"

"${py_bin}" "${this_dir}/profiling_proc.py" "${csv_path}" "${this_dir}/profiling_config.yml"
#"/home/rdahl/.pyenv/versions/3.8.5/envs/dex/bin/python" "${this_dir}/profiling_proc.py" "${csv_path}"

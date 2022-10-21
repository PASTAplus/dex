#!/usr/bin/env bash

# Add a directory path to the front of PATH, or another ":" delimited env var.
# - Converts relative path to absolute.
# - No-op if the path does not exist. Uses current dir if path not given.
# - Optional second argument allows acting on another env var.
function padd() {
  dir_path="$1"
  if [[ -n "$2" ]]; then env_var="$2"; else env_var="PATH"; fi
  abs_path="$(realpath --canonicalize-existing --quiet "$dir_path")"
  if [ $? -eq 0 ]; then
    printf -v "$env_var" "%s" "$abs_path${!env_var:+:${!env_var}}"
    export $"env_var"
  fi
}

# Returns the absolute path to the directory of the caller.
function here() {
  echo -n "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
}

#eval "$(command conda 'shell.bash' 'hook' 2> /dev/null)"
#conda init bash
#conda activate dex

#padd "$(here)" PYTHONPATH
#padd "$(here)/dex" PYTHONPATH

export FLASK_RUN_PORT=5000
export FLASK_RUN_HOST=0.0.0.0
export FLASK_ENV='development'
export FLASK_DEBUG='1'
export WERKZEUG_DEBUG_PIN='off'

flask run # | tee -a flask.run.out

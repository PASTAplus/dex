#!/usr/bin/env bash

# Add a development version of DeX to the syspath of the currently
# active Python environment.

# Returns the absolute path to the directory of the caller.
function here() {
  echo -n "$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
}

printf '%s\n' "$(realpath "$(here)/..")" \
  > "$(pyenv prefix)/lib/python3.9/site-packages/dex.pth"

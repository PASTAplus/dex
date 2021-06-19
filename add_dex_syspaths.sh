#!/usr/bin/env bash

# Add a development version of Dex to the syspath.

cat <<EOF >$(pyenv prefix)/lib/python3.9/site-packages/dex.pth
/home/dahl/dev/dex
EOF

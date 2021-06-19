#!/usr/bin/env bash

# Monitor the filesystem and do an HTTP GET request to the given URL whenever a file
# changes.

while true; do
  url="$1"
  printf 'url: %s\n' "$url"
  curl "$url"
  inotifywait --recursive --event close_write .
done

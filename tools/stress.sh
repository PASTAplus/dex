#!/usr/bin/env bash

# Stress test a view in Dex

url='http://127.0.0.1:5000/dex/subset/1'

for i in {1..10} ; do
  printf 'Cycle: %s\n' $i

  for j in {1..20} ; do
    printf 'Parallel: %s\n' $j
    ( curl --no-progress-bar "$url" > /dev/null; ) &
  done

  wait
done

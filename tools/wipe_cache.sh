#!/usr/bin/env bash

# Wipe the cache and database.

#set -e

test -e schema.sql || {
  echo 'Run with current dir at the dex root'
  exit
}

dirs=('../cache' '../dex-cache')

# Skip global cache files
#for p in "${dirs[@]}"; do
#  find "$p" -print -delete
#done

# All
for p in "${dirs[@]}"; do
  find "$p" -print -delete
  mkdir -p "$p"
done

rm sqlite.db
sqlite3 <schema.sql sqlite.db

echo Success!

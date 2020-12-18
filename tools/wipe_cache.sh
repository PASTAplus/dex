#!/usr/bin/env bash

# Wipe the cache and database.

set -e

test -e create_db.sql || { echo 'Run with current dir at the dex root'; exit; }

rm -rf ../cache
rm sqlite.db
sqlite3 < create_db.sql sqlite.db

echo Success!


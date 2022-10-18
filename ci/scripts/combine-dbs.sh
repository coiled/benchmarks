#!/bin/bash
set -euxo pipefail

DB_NAME=${DB_NAME:-'benchmark.db'}

# Possibly apply migrations to the main db,
# working on a copy with a known name so it's
# easier to refer to in sqlite
if [ -f "$DB_NAME" ]; then
  cp "$DB_NAME" benchmark.tmp.db
else
  sqlite3 benchmark.tmp.db "VACUUM;"
fi
DB_NAME=benchmark.tmp.db alembic upgrade head

# Merge in the individual job dbs into our working copy
for FILE in $(find benchmarks -name "*.db")
do
  # Skip the output DB if we see it
  if [ ${FILE##*/} == $DB_NAME ]; then
    echo "Skipping $FILE"
    continue
  fi
  echo "Processing $FILE"
  # Copy the individual table into the primary one. We make an intermediate
  # temp table so that we can null out the primary keys and reset the
  # autoincrementing
  sqlite3 "$FILE" <<EOF
attach "benchmark.tmp.db" as lead;
create temporary table tmp as select * from main.test_run;
update tmp set id=NULL;
insert into lead.test_run select * from tmp;
detach database lead;
EOF
done

mv benchmark.tmp.db "$DB_NAME"

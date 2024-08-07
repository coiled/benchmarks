#!/bin/bash
set -euxo pipefail

DB_NAME=${DB_NAME:-'benchmark.db'}

alembic upgrade head

# Delete old records and vacuum to reduce on-disk size
sqlite3 "$DB_NAME" <<EOF
DELETE FROM test_run WHERE session_id not in (SELECT DISTINCT session_id FROM test_run WHERE start > date('now', '-90 days'));
VACUUM;
EOF
# Merge in the individual job dbs into our working copy
for FILE in $(find . -name "*.db")
do
  # Skip the output DB if we see it
  if [ ${FILE##*/} == $DB_NAME ]; then
    echo "Skipping $FILE"
    continue
  fi
  echo "Processing $FILE"
  DB_NAME=$FILE alembic upgrade head
  # Copy the individual table into the primary one. We make an intermediate
  # temp table so that we can null out the primary keys and reset the
  # autoincrementing
  for tab in "tpch_run" "test_run"
  do
  sqlite3 "$FILE" <<EOF
attach "$DB_NAME" as lead;
create temporary table tmp as select * from main.$tab;
update tmp set id=NULL;
insert into lead.$tab select * from tmp;
detach database lead;
EOF
  done
done

sqlite3 "$DB_NAME" "VACUUM;"

#!/usr/bin/env bash

# This script can be used to determine whether there will be any gorm db schema changes from a git branch
# It prints and outputs to the DIFF variable whether the gorm auto-migration will run any SQL that modifies the db

git_branch=$1
# The db holding the current schema of the db.
baseline_db_conn_string=$2
# A temporary db is used to copy the schema of the baseline db, so that an auto-migration can be run against it without
# risk of corrupting the baseline db.
db_copy_conn_string=$3

# Parse db connection strings
db_driver=$(echo "$baseline_db_conn_string" | sed -n "s/\(\S*\):\/\/.*$/\1/p")
copy_db_driver=$(echo "$db_copy_conn_string" | sed -n "s/\(\S*\):\/\/.*$/\1/p")

if [ "$db_driver" != "$copy_db_driver" ]; then
  echo "DB driver must match between the baseline and copy dbs"
  exit 1
fi

if [[ "$db_driver" == "sqlite3" ]]; then
  db=$(echo "$baseline_db_conn_string" | sed -n "s/^sqlite3:\/\/\(\S*\)$/\1/p")
  copy_db=$(echo "$db_copy_conn_string" | sed -n "s/^sqlite3:\/\/\(\S*\)$/\1/p")

  # Copy schema from baseline db to db copy
  sqlite3 "$db" ".schema --nosys" | sqlite3 "$copy_db"
elif [[ "$db_driver" == "mysql" ]]; then
  # Expecting something like: mysql://buildbuddy-dev:password@tcp(127.0.0.1:3308)/buildbuddy_dev
  username=$(echo "$baseline_db_conn_string" | sed -n "s/^.*:\/\/\(\S*\):.*@.*$/\1/p")
  password=$(echo "$baseline_db_conn_string" | sed -n "s/^.*:\/\/.*:\(\S*\)@.*$/\1/p")
  ip=$(echo "$baseline_db_conn_string" | sed -n "s/^.*(\(\S*\):.*$/\1/p")
  port=$(echo "$baseline_db_conn_string" | sed -n "s/^.*(.*:\(\S*\)).*$/\1/p")
  db_name=$(echo "$baseline_db_conn_string" | sed -n "s/^.*)\/\(\S*\).*$/\1/p")

  copy_username=$(echo "$db_copy_conn_string" | sed -n "s/^.*:\/\/\(\S*\):.*$/\1/p")
  copy_password=$(echo "$db_copy_conn_string" | sed -n "s/^.*:\/\/.*:\(\S*\)@.*$/\1/p")
  copy_ip=$(echo "$db_copy_conn_string" | sed -n "s/^.*(\(\S*\)).*$/\1/p")
  copy_db_name=$(echo "$db_copy_conn_string" | sed -n "s/^.*)\/\(\S*\).*$/\1/p")

  # Copy schema from baseline db to db copy. -d flag to not copy any data
  mysqldump -u "$username" -p"$password" -h "$ip" -P "$port" -d  --set-gtid-purged=OFF "$db_name" | \
  mysql -u "$copy_username" -p"$copy_password" -h "$copy_ip" "$copy_db_name"
else
 echo "Invalid db driver"
 exit 1
fi

# Checkout new branch and run auto_migration on db copy
git checkout "$git_branch"
schema_changes=$(bazel run //enterprise/server -- --database.data_source="$db_copy_conn_string" --database.print_schema_changes_and_exit=true)

echo "$schema_changes"
export schema_changes
#!/usr/bin/env bash

# This script can be used to determine whether there will be any gorm db schema changes from a git branch
#
# Example usage:
# Passwords can be found in buildbuddy-internal/enterprise/config/buildbuddy.dev.yaml
# # Port-forward dev mysql server so it can be accessed locally
# kubectl --namespace=tools-dev port-forward deployment/sqlproxy 3308:3306
# # Run script for mysql
# schema_changes=$(./print_schema_changes.sh MY_GIT_BRANCH "mysql://buildbuddy-dev:PASSWORD@tcp(127.0.0.1:3308)/buildbuddy_dev")
#
# # Port-forward dev clickhouse server so it can be accessed locally
# kubectl -n clickhouse-operator-dev port-forward chi-repl-dev-replicated-0-0-0 9001:9000
# # Run script for clickhouse
# schema_changes=$(./print_schema_changes.sh MY_GIT_BRANCH "clickhouse://buildbuddy_dev:PASSWORD@localhost:9001/buildbuddy_dev")

git_branch=$1
db_conn_string=$2

git checkout "$git_branch" 1>/dev/null

# Parse db connection string
db_driver=$(echo "$db_conn_string" | sed -n "s/\(\S*\):\/\/.*$/\1/p")
if [[ "$db_driver" == "clickhouse" ]]; then
  schema_changes=$(bazel run //enterprise/server -- --olap_database.data_source="$db_conn_string" --olap_database.print_schema_changes_and_exit=true)
else
  schema_changes=$(bazel run //enterprise/server -- --database.data_source="$db_conn_string" --database.print_schema_changes_and_exit=true)
fi

if [[ "$schema_changes" != "" ]]; then
  echo "$schema_changes"
fi
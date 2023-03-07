# This script can be used to determine whether there will be any Clickhouse schema changes from a git branch
# It prints and outputs to the clickhouse_schema_changes variable whether the gorm auto-migration will run any SQL that modifies the db
#
# Example usage:
# # Run local clickhouse server
# ./clickhouse server
# # Create local clickhouse database
#  ./clickhouse client -q "create database buildbuddy_local"
# # Run script
# source print_clickhouse_schema_changes.sh MY_GIT_BRANCH "clickhouse://default:@127.0.0.1:9000/buildbuddy_local"
# echo $clickhouse_schema_changes

git_branch=$1
# A temporary db is used to copy the schema of the baseline db, so that an auto-migration can be run against it without
# risk of corrupting the baseline db.
db_copy_conn_string=$2
# For clickhouse, there is no built-in way to copy the schema of the baseline db to the db copy. Instead we can run
# the auto-migration against the master commit to initialize the db copy's schema so that it matches the baseline
clickhouse_baseline_git_branch=$3

# Migrate db copy to have schema of the baseline branch
git checkout "$clickhouse_baseline_git_branch"
bazel run //enterprise/server -- --olap_database.data_source="$db_copy_conn_string" --olap_database.print_schema_changes_and_exit=true  > /dev/null 2>&1

# TODO: Remove when buildbuddy-internal#2096 is fixed
# When auto-migrating clickhouse tables, gorm runs MODIFY COLUMN on all columns even if their types have not changed
# To determine whether a meaningful migration is taking place, we need to generate the no-op migration diff against the
# baseline, and check whether that matches the migration diff on the new branch
baseline_schema_changes=$(bazel run //enterprise/server -- --olap_database.data_source="$db_copy_conn_string" --olap_database.print_schema_changes_and_exit=true 2>/dev/null)

# Checkout new branch and run auto_migration on db copy
git checkout "$git_branch"
new_schema_changes=$(bazel run //enterprise/server -- --olap_database.data_source="$db_copy_conn_string" --olap_database.print_schema_changes_and_exit=true 2>/dev/null)

clickhouse_schema_changes=$(diff <(echo "$baseline_schema_changes") <(echo "$new_schema_changes"))

echo "$clickhouse_schema_changes"
export clickhouse_schema_changes

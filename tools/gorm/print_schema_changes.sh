# This script can be used to determine whether there will be any gorm db schema changes from a git branch
#
# Example usage:
# # Port-forward dev mysql server so it can be accessed locally
# kubectl --namespace=tools-dev port-forward deployment/sqlproxy 3308:3306
# # Run script. Dev mysql password can be found in buildbuddy-internal/enterprise/config/buildbuddy.dev.yaml
# schema_changes=$(./print_schema_changes.sh MY_GIT_BRANCH "mysql://buildbuddy-dev:PASSWORD@tcp(127.0.0.1:3308)/buildbuddy_dev")

git_branch=$1
db_conn_string=$2
# TODO: Remove when buildbuddy-internal#2096 is fixed
# baseline_git_branch is only needed when using the script for clickhouse
# When auto-migrating clickhouse tables, gorm runs MODIFY COLUMN on all columns even if their types have not changed
# To determine whether a meaningful migration is taking place, we need to compare the migration diff against the
# baseline
baseline_git_branch=$3

# If running on a Mac, you may need to uncomment the following for the string parsing to work correctly:
# alias sed="gsed"

# Parse db connection string
db_driver=$(echo "$db_conn_string" | sed -n "s/\(\S*\):\/\/.*$/\1/p")
if [[ "$db_driver" == "clickhouse" ]]; then
  if [[ "$baseline_git_branch" == "" ]]; then
    echo "When running this script with clickhouse, you must pass in a baseline git branch that contains the current
    schema of the database in order to generate a meaningful diff. See buildbuddy-internal#2096 for more info."
    exit 1
  fi

  git checkout "$baseline_git_branch" >/dev/null 2>&1
  baseline_migration_diff=$(bazel run //enterprise/server -- --olap_database.data_source="$db_conn_string" --olap_database.print_schema_changes_and_exit=true 2>/dev/null)
  git checkout "$git_branch" >/dev/null 2>&1
  new_migration_diff=$(bazel run //enterprise/server -- --olap_database.data_source="$db_conn_string" --olap_database.print_schema_changes_and_exit=true 2>/dev/null)

  schema_changes=$(diff <(echo "$baseline_migration_diff") <(echo "$new_migration_diff"))
else
  git checkout "$git_branch" >/dev/null 2>&1
  schema_changes=$(bazel run //enterprise/server -- --database.data_source="$db_conn_string" --database.print_schema_changes_and_exit=true 2>/dev/null)
fi

if [[ "$schema_changes" != "" ]]; then
  echo "$schema_changes"
fi
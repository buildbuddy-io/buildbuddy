# This script can be used to determine whether there will be any gorm db schema changes from a git branch
# It prints and outputs to the schema_changes variable whether the gorm auto-migration will run any SQL that modifies the db
#
# Example usage:
# # Port-forward dev mysql server so it can be accessed locally
# kubectl --namespace=tools-dev port-forward deployment/sqlproxy 3308:3306
# # Run script. Dev mysql password can be found in buildbuddy-internal/enterprise/config/buildbuddy.dev.yaml
# source print_schema_changes.sh MY_GIT_BRANCH "mysql://buildbuddy-dev:PASSWORD@tcp(127.0.0.1:3308)/buildbuddy_dev"
# echo $schema_changes

git_branch=$1
db_conn_string=$2

git checkout "$git_branch"
schema_changes=$(bazel run //enterprise/server -- --database.data_source="$db_conn_string" --database.print_schema_changes_and_exit=true 2>/dev/null)

if [[ "$schema_changes" != "" ]]; then
  echo "Auto-migration schema changes:"
  echo "$schema_changes"
else
  echo "No auto-migration schema changes."
fi

export schema_changes

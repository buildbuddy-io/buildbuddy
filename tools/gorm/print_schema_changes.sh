# This script can be used to determine whether there will be any gorm db schema changes from a git branch
#
# Example usage:
# # Port-forward dev mysql server so it can be accessed locally
# kubectl --namespace=tools-dev port-forward deployment/sqlproxy 3308:3306
# # Run script. Dev mysql password can be found in buildbuddy-internal/enterprise/config/buildbuddy.dev.yaml
# schema_changes=$(./print_schema_changes.sh MY_GIT_BRANCH "mysql://buildbuddy-dev:PASSWORD@tcp(127.0.0.1:3308)/buildbuddy_dev")

git_branch=$1
db_conn_string=$2

git checkout "$git_branch" >/dev/null 2>&1
schema_changes=$(bazel run //enterprise/server -- --database.data_source="$db_conn_string" --database.print_schema_changes_and_exit=true 2>/dev/null)

if [[ "$schema_changes" != "" ]]; then
  echo "$schema_changes"
fi
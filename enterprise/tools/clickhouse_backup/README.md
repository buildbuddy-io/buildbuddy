# clickhouse_backup

The `clickhouse_backup` tool in this package allows creating and restoring
ClickHouse database backups. It can execute BACKUP or RESTORE queries
against a configured ClickHouse database.

Before using the tool, ClickHouse must first be configured with a backup
disk, which can either be a local disk or an S3-compatible disk. See
enterprise/tools/clickhouse_cluster/gcs_backup.xml for an example.

The tool must be configured with `storage.*` flags to match the configured
backup disk (e.g. `storage.disk.*` for a local disk, or `storage.gcs.*`
for a GCS disk). Be sure to also set `storage.path_prefix` if backups are
stored in a subdirectory.

## Creating a backup

The `create` subcommand creates a backup. By default, it takes a full
backup on the first day of the month. On other days of the month, it takes
a backup based on the previous day if it exists.

Example command:

```shell
bazel run -- //enterprise/tools/clickhouse_backup \
  --olap_database.data_source=clickhouse://user:password@host:port/my_database \
  --storage.gcs.credentials="$GCS_CREDENTIALS_JSON" \
  --storage.gcs.project_id=acme-inc \
  --storage.gcs.bucket=acme-inc-clickhouse-backups \
  --storage.path_prefix=v1 \
  create \
  --database=my_database \
  --backup_disk_name=gcs_backup
```

## Restoring a backup

The `restore` command restores from an existing backup. There are a few
different ways to use it, depending on how you want to restore the lost
data.

The simplest approach is to first drop the destination database (by
separately running a manual ClickHouse query), and then restore the entire
database from the most recent backup. If the database contains any new
rows, they will be lost:

```shell
clickhouse-client 'DROP DATABASE my_database ON CLUSTER my_cluster'
bazel run -- //enterprise/tools/clickhouse_backup \
  --olap_database.data_source=clickhouse://user:password@host:port/my_database \
  --storage.gcs.credentials="$GCS_CREDENTIALS_JSON" \
  --storage.gcs.project_id=acme-inc \
  --storage.gcs.bucket=acme-inc-clickhouse-backups \
  --storage.path_prefix=v1 \
  restore \
  --backup_disk_name=gcs_backup \
  --backup_database=my_database \
  --destination_database=my_database \
  --all_tables
```

Another option is to restore a single table, which can be useful if you
accidentally dropped or truncated a table. First drop the table, then run
a `restore` command specifying a single `--table` to be restored:

```shell
clickhouse-client 'DROP TABLE my_database.MyTable ON CLUSTER my_cluster'
bazel run -- //enterprise/tools/clickhouse_backup \
  --olap_database.data_source=clickhouse://user:password@host:port/my_database \
  --storage.gcs.credentials="$GCS_CREDENTIALS_JSON" \
  --storage.gcs.project_id=acme-inc \
  --storage.gcs.bucket=acme-inc-clickhouse-backups \
  --storage.path_prefix=v1 \
  restore \
  --backup_disk_name=gcs_backup \
  --backup_database=my_database \
  --destination_database=my_database \
  --table=MyTable
```

In rare circumstances, it might make sense to _append_ backups into an
existing table, by using the `--allow_non_empty_tables` flag. For example,
if a backup was taken 12 hours ago, and a table was accidentally deleted 5
hours ago, and the deletion went unnoticed until now, then the table
contains 5 hours of useful data that may be worth keeping. So, in this
case, `--allow_non_empty_tables` might be appropriate.

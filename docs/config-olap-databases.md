---
id: config-olap-database
title: OLAP Database Configuration
sidebar_label: OLAP Database
---

## Introduction

To speed up the analysis of historical build event data, BuildBuddy can be configured to use ClickHouse as an OLAP database, in addition to the primary SQL database required for core functionality.

Setting up ClickHouse is completely optional when using BuildBuddy.
BuildBuddy does not require ClickHouse for its core features, including the build results UI, remote cache, and remote execution system.

However, some UI features, such as Trends, Drilldown, Test Grid, Flakes UI, Tags filtering, and Audit Logging, may require ClickHouse.
Without a configured ClickHouse instance, these features will either be missing from the UI, or will be missing some features and may not scale to larger amounts of data.

## Options

**Optional**

`olap_database:` The OLAP (online analytical processing) database section configures the OLAP database that BuildBuddy uses to enable the Trends page. **Optional**

- `data_source` This is a connection string used by the database driver to connect to the database. ClickHouse database is supported.

- `enable_data_replication` If ClickHouse is using a [cluster deployment](https://clickhouse.com/docs/en/architecture/cluster-deployment), this will enable data replication within the cluster.

## Example sections

Example single-instance ClickHouse configuration:

```yaml
olap_database:
  data_source: "clickhouse://buildbuddy_user:pAsSwOrD@12.34.56.78:9000/buildbuddy_db"
```

Example ClickHouse cluster configuration:

```yaml
olap_database:
  data_source: "clickhouse://buildbuddy_user:pAsSwOrD@12.34.56.78:9000/buildbuddy_db"
  enable_data_replication: true
```

## Sizing

2 CPUs and 50 GiB memory can handle ~150 insert QPS (normal) / 600 QPS (peak).
Disk usage can be estimated as follows:

| Table              | avg_bytes_per_row |
| ------------------ | ----------------- |
| Executions         | 260               |
| Invocations        | 107               |
| TestTargetStatuses | 23.0              |

Note: `TestTargetStatuses` corresponds to `Targets` and `TargetStatuses` in MySQL.
An additional 20% disk is recommended to be reserved to handle additional disk usage during merging.

## Feature Configuration

After configuring the ClickHouse connection, you'll need to enable specific feature flags to use the OLAP-powered UI features:

### Trends and Drilldown

The Trends page provides historical analysis of build performance and patterns. An example of how to use Trends and Drilldown can be found at [this blog post](https://www.buildbuddy.io/blog/debugging-slow-bazel-builds/#5-was-there-a-change-in-my-project-that-could-explain-increasing-build-times).
Most of the features are enabled by default once you set up the OLAP
database.

```yaml
app:
  trends_summary_enabled: true # Show summary section at the top of trends UI (optional, default: false)
```

### Tags Filtering

Tags allow you to categorize and filter invocations. While tags can be used without ClickHouse, it is highly recommended to use them with ClickHouse for better performance.

```yaml
app:
  tags_enabled: true # Enable setting tags on invocations via build_metadata (default: false)
  tags_ui_enabled: true # Expose tags data and filtering in the UI (default: false)
```

### Test Grid

The Test Grid provides a comprehensive view of test results over time.

```yaml
app:
  enable_target_tracking: true # Enable target tracking (default: false)
  test_grid_v2_enabled: true # Enable Test Grid V2 (default: true)
  enable_write_test_target_statuses_to_olap_db: true # Write test target statuses to ClickHouse (default: false)
```

### Flakes

The Flakes UI shows "Flaky" and "Likely Flaky" test targets. This requires
the same configuration as Test Grid plus turning on a new flag.

```yaml
app:
  enable_target_tracking: true # Enable target tracking (default: false)
  test_grid_v2_enabled: true # Enable Test Grid V2 (default: true)
  enable_write_test_target_statuses_to_olap_db: true # Write test target statuses to ClickHouse (default: false)
  target_flakes_ui_enabled: true # Turns on the Flakes UI (default: false)
```

### Audit Logging

Audit logs track administrative events and require ClickHouse for storage.

```yaml
app:
  audit_logs_enabled: true # Log administrative events to audit log (default: false)
  audit_logs_ui_enabled: true # Make audit logs accessible from the sidebar (default: false)
```

### Remote Execution Data Storage

By default, BuildBuddy automatically writes remote execution data to ClickHouse when an OLAP database is configured. You can also enable reading execution data from ClickHouse instead of the primary database for better performance.

```yaml
app:
  enable_write_executions_to_olap_db: true # Write execution data to ClickHouse (default: true)

remote_execution:
  olap_reads_enabled: true # Read executions from ClickHouse (and in-progress execution info from Redis) (default: false)
  write_execution_progress_state_to_redis: true # Write execution progress updates to Redis (default: false)
  write_executions_to_primary_db: false # Write executions to the primary DB (default: true)
  primary_db_reads_enabled: false # Read executions from the primary database (default: true)
```

**Note:** `write_execution_progress_state_to_redis: true` needs to be set in order for in-progress executions to show up in the UI.

When `olap_reads_enabled` and `write_execution_progress_state_to_redis` are enabled, BuildBuddy will:

- Read completed execution data from ClickHouse
- Read in-progress execution information from Redis
- Fall back to the primary database for executions not found in ClickHouse

**Migration considerations:** After migrating to ClickHouse, primary DB writes can be disabled (`write_executions_to_primary_db: false`), so that executions are only written to Redis + ClickHouse, reducing load on the primary DB.

It may also be desired to set `primary_db_reads_enabled: false` to skip primary DB reads once primary DB writes are disabled. This will speed up queries for the executions page, but older invocations created before migrating to ClickHouse will not have any execution data displayed in the UI.

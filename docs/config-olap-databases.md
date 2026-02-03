---
id: config-olap-database
title: OLAP Database Configuration
sidebar_label: OLAP Database
---

## Introduction

To speed up the analysis of historical build event data, BuildBuddy can be configured to use ClickHouse as an OLAP database, in addition to the primary SQL database required for core functionality.

Setting up ClickHouse is completely optional when using BuildBuddy.
BuildBuddy does not require ClickHouse for its core features, including the build results UI, remote cache, and remote execution system.

However, some UI features, such as Trends, Drilldown, Test Grid, Tags filtering, and Audit Logging, may require ClickHouse.
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

## Feature Configuration

After configuring the ClickHouse connection, you'll need to enable specific feature flags to use the OLAP-powered UI features:

### Trends

The Trends page provides historical analysis of build performance and patterns.

```yaml
app:
  trends_heatmap_enabled: true           # Enable heatmap UI for exploring build trends (default: true)
  trends_summary_enabled: true           # Show summary section at the top of trends UI (optional, default: false)
  execution_search_enabled: true         # Fetch lists of executions from OLAP DB in trends UI (default: true)
```

### Tags Filtering

Tags allow you to categorize and filter invocations.

```yaml
app:
  tags_enabled: true                     # Enable setting tags on invocations via build_metadata (default: false)
  tags_ui_enabled: true                  # Expose tags data and filtering in the UI (default: false)
  fetch_tags_drilldown_data: true        # Enable tag-based drilldowns (optional, default: true)
```

### Test Grid

The Test Grid provides a comprehensive view of test results over time.

```yaml
app:
  test_grid_v2_enabled: true             # Enable Test Grid V2 (default: true)
```

### Audit Logging

Audit logs track administrative events and require ClickHouse for storage.

```yaml
app:
  audit_logs_enabled: true               # Log administrative events to audit log (default: false)
  audit_logs_ui_enabled: true            # Make audit logs accessible from the sidebar (default: false)
```

### Remote Execution Data Storage

By default, BuildBuddy automatically writes remote execution data to ClickHouse when an OLAP database is configured. You can also enable reading execution data from ClickHouse instead of the primary database for better performance.

```yaml
app:
  enable_write_executions_to_olap_db: true   # Write execution data to ClickHouse (default: true)

remote_execution:
  olap_reads_enabled: true                   # Read executions from ClickHouse (and in-progress execution info from Redis) (default: false)
```

**Note:** When `olap_reads_enabled` is enabled, BuildBuddy will:
- Read completed execution data from ClickHouse
- Read in-progress execution information from Redis
- Fall back to the primary database for executions not found in ClickHouse

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

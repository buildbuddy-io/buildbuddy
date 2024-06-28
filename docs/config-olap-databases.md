---
id: config-olap-database
title: OLAP Database Configuration
sidebar_label: OLAP Database
---

## Introduction

To speed up the analysis of historical build event data, BuildBuddy relies on ClickHouse, an OLAP database solution.

Deploying ClickHouse is optional when using BuildBuddy
BuildBuddy does not require ClickHouse for its core features, such as Build Event Service, Remote Cache, and Remote Execution.

However, some UI features, such as Trends, Drilldown, and Test Grid, do rely on ClickHouse.
Without a configured ClickHouse instance, these features will either not be displayed on our UI or will operate in a different mode.

## Options

**Optional**

`olap_database:` The OLAP (online analytical processing) database section configures the OLAP database that BuildBuddy uses to enable the Trends page. **Optional**

- `data_source` This is a connection string used by the database driver to connect to the database. ClickHouse database is supported.

- `enable_data_replication` If ClickHouse is using a [Cluster Deployment](https://clickhouse.com/docs/en/architecture/cluster-deployment), this will enable data replication within the cluster.

## Example sections

Example single instance clickhouse configuration

```
olap_database:
  data_source: "clickhouse://buildbuddy_user:pAsSwOrD@12.34.56.78:9000/buildbuddy_db"
```

Example cluster clickhouse configuration

```
olap_database:
  data_source: "clickhouse://buildbuddy_user:pAsSwOrD@12.34.56.78:9000/buildbuddy_db"
  enable_data_replication: true
```

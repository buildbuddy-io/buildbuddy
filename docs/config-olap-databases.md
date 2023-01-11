---
id: config-olap-database
title: OLAP Database Configuration
sidebar_label: OLAP Database
---

## Section

`olap_database:` The OLAP (online analytical processing) database section configures the OLAP database that BuildBuddy uses to enable the Trends page. **Optional**

Note: in order to use OLAP database for the Trends page, `app.enable_read_from_olap_db` and
`app.enable_write_to_olapdb` needs to be set to `true`

## Options

**Optional**

- `data_source` This is a connection string used by the database driver to connect to the database. ClickHouse database is supported.
- `enable_data_replication` If true, data replication is enabled.

## Example sections

```
app:
  enable_read_from_olap_db: true
  enable_write_to_olap_db: true
olap_database:
  data_source: "clickhouse://buildbuddy_user:pAsSwOrD@12.34.56.78:9000/buildbuddy_db"
  enable_data_replication: true
```

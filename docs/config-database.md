<!--
{
  "name": "Database",
  "category": "5eed3e2ace045b343fc0a328"
}
-->
# Database Configuration

## Section

```database:``` The database section configures the database that BuildBuddy stores metadata in. **Required**

## Options

**Required**

* ```data_source```  This is a connection string used by the database driver to connect to the database. MySQL and SQLite databases are supported.

## Example sections

### SQLite

```
database:
  data_source: "sqlite3:///tmp/buildbuddy.db"
```

### MySQL

```
database:
  data_source: "mysql://buildbuddy_user:pAsSwOrD@tcp(12.34.56.78)/buildbuddy_db"
```
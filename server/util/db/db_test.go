package db_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/stretchr/testify/require"
)

func TestParseDataSource(t *testing.T) {
	ctx := context.Background()

	_, err := db.ParseDatasource(ctx, "", &db.AdvancedConfig{})
	require.ErrorContains(t, err, "no database configured")

	_, err = db.ParseDatasource(ctx, "foo", &db.AdvancedConfig{})
	require.ErrorContains(t, err, "malformed")

	ds, err := db.ParseDatasource(ctx, "foo://bar/baz?abc=xyz", &db.AdvancedConfig{})
	require.NoError(t, err)
	require.Equal(t, "foo", ds.DriverName())
	dsn, err := ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "bar/baz?abc=xyz", dsn)

	ds, err = db.ParseDatasource(ctx, "", &db.AdvancedConfig{
		Driver:   "sqlite3",
		Endpoint: "/tmp/mydb",
	})
	require.NoError(t, err)
	require.Equal(t, "sqlite3", ds.DriverName())
	dsn, err = ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "/tmp/mydb", dsn)

	ds, err = db.ParseDatasource(ctx, "", &db.AdvancedConfig{
		Driver:   "sqlite3",
		Endpoint: "/tmp/mydb",
		Params:   "foo=bar",
	})
	require.NoError(t, err)
	require.Equal(t, "sqlite3", ds.DriverName())
	dsn, err = ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "/tmp/mydb?foo=bar", dsn)

	ds, err = db.ParseDatasource(ctx, "", &db.AdvancedConfig{
		Driver:   "mysql",
		Endpoint: "host:port",
		Username: "user",
		Password: "pass",
		DBName:   "db",
	})
	require.NoError(t, err)
	require.Equal(t, "mysql", ds.DriverName())
	dsn, err = ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "user:pass@tcp(host:port)/db?sql_mode=ANSI_QUOTES", dsn)

	ds, err = db.ParseDatasource(ctx, "", &db.AdvancedConfig{
		Driver:   "mysql",
		Endpoint: "host:port",
		Username: "user",
		Password: "pass",
		DBName:   "db",
		Params:   "foo=bar",
	})
	require.NoError(t, err)
	require.Equal(t, "mysql", ds.DriverName())
	dsn, err = ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "user:pass@tcp(host:port)/db?foo=bar&sql_mode=ANSI_QUOTES", dsn)

	ds, err = db.ParseDatasource(ctx, "", &db.AdvancedConfig{
		Driver:   "postgresql",
		Endpoint: "host:9097",
		Username: "user",
		Password: "pass",
		DBName:   "db",
	})
	require.NoError(t, err)
	require.Equal(t, "postgresql", ds.DriverName())
	dsn, err = ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "postgres://user:pass@host:9097/db", dsn)

	ds, err = db.ParseDatasource(ctx, "", &db.AdvancedConfig{
		Driver:   "postgresql",
		Endpoint: "host:9097",
		Username: "user",
		Password: "pass",
		DBName:   "db",
		Params:   "foo=bar",
	})
	require.NoError(t, err)
	require.Equal(t, "postgresql", ds.DriverName())
	dsn, err = ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "postgres://user:pass@host:9097/db?foo=bar", dsn)
}

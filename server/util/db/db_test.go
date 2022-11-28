package db_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/stretchr/testify/require"
)

func TestParseDataSource(t *testing.T) {
	_, err := db.ParseDatasource("", &db.AdvancedConfig{})
	require.ErrorContains(t, err, "no database configured")

	_, err = db.ParseDatasource("foo", &db.AdvancedConfig{})
	require.ErrorContains(t, err, "malformed")

	ds, err := db.ParseDatasource("foo://bar/baz?abc=xyz", &db.AdvancedConfig{})
	require.NoError(t, err)
	require.Equal(t, "foo", ds.DriverName())
	dsn, err := ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "bar/baz?abc=xyz", dsn)

	ds, err = db.ParseDatasource("", &db.AdvancedConfig{
		Driver:   "sqlite3",
		Endpoint: "/tmp/mydb",
	})
	require.NoError(t, err)
	require.Equal(t, "sqlite3", ds.DriverName())
	dsn, err = ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "/tmp/mydb", dsn)

	ds, err = db.ParseDatasource("", &db.AdvancedConfig{
		Driver:   "sqlite3",
		Endpoint: "/tmp/mydb",
		Params:   "foo=bar",
	})
	require.NoError(t, err)
	require.Equal(t, "sqlite3", ds.DriverName())
	dsn, err = ds.DSN()
	require.NoError(t, err)
	require.Equal(t, "/tmp/mydb?foo=bar", dsn)

	ds, err = db.ParseDatasource("", &db.AdvancedConfig{
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
	require.Equal(t, "user:pass@tcp(host:port)/db", dsn)

	ds, err = db.ParseDatasource("", &db.AdvancedConfig{
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
	require.Equal(t, "user:pass@tcp(host:port)/db?foo=bar", dsn)
}

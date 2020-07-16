package db

import (
	"flag"
	"fmt"
	"strings"

	"github.com/jinzhu/gorm"
	// We support MySQL (preferred), Postgresql, and Sqlite3
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"
	// Allow for "cloudsql" type connections that support workload identity.
	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
)

const (
	sqliteDialect = "sqlite3"
)

var (
	autoMigrateDB = flag.Bool("auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
)

type DBHandle struct {
	*gorm.DB
}

func NewDBHandle(dialect string, args ...interface{}) (*DBHandle, error) {
	gdb, err := gorm.Open(dialect, args...)
	if err != nil {
		return nil, err
	}
	gdb.SingularTable(true)
	gdb.LogMode(false)
	if *autoMigrateDB {
		gdb.AutoMigrate(tables.GetAllTables()...)
		if err := tables.ManualMigrate(gdb); err != nil {
			return nil, err
		}
	}
	// SQLITE Special! To avoid "database is locked errors":
	if dialect == sqliteDialect {
		gdb.Exec("PRAGMA journal_mode=WAL;")
	}
	return &DBHandle{
		DB: gdb,
	}, nil
}

func GetConfiguredDatabase(c *config.Configurator) (*DBHandle, error) {
	datasource := c.GetDBDataSource()
	if datasource != "" {
		parts := strings.SplitN(datasource, "://", 2)
		dialect, connString := parts[0], parts[1]
		return NewDBHandle(dialect, connString)
	}
	return nil, fmt.Errorf("No database configured -- please specify at least one in the config")
}

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
	dialect string
}

func NewDBHandle(dialect string, args ...interface{}) (*DBHandle, error) {
	gdb, err := gorm.Open(dialect, args...)
	if err != nil {
		return nil, err
	}
	gdb.SingularTable(true)
	gdb.LogMode(false)
	if *autoMigrateDB {
		if err := tables.ManualMigrate(gdb); err != nil {
			return nil, err
		}
		gdb.AutoMigrate(tables.GetAllTables()...)
	}
	// SQLITE Special! To avoid "database is locked errors":
	if dialect == sqliteDialect {
		gdb.DB().SetMaxOpenConns(1)
		gdb.Exec("PRAGMA journal_mode=WAL;")
	}
	return &DBHandle{
		DB:      gdb,
		dialect: dialect,
	}, nil
}

func (h *DBHandle) DateFromUsecTimestamp(fieldName string) string {
	if h.dialect == sqliteDialect {
		return "DATE(" + fieldName + "/1000000, 'unixepoch')"
	}
	return "DATE(FROM_UNIXTIME(" + fieldName + "/1000000))"
}

func GetConfiguredDatabase(c *config.Configurator) (*DBHandle, error) {
	datasource := c.GetDBDataSource()
	if datasource != "" {
		parts := strings.SplitN(datasource, "://", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed db connection string")
		}
		dialect, connString := parts[0], parts[1]
		return NewDBHandle(dialect, connString)
	}
	return nil, fmt.Errorf("No database configured -- please specify at least one in the config")
}

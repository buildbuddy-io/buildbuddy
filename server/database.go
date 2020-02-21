package database

import (
	"flag"
	"fmt"
	"strings"

	"github.com/jinzhu/gorm"
	// We support MySQL (preferred), Postgresql, and Sqlite3
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"

	"github.com/tryflame/buildbuddy/server/config"
	"github.com/tryflame/buildbuddy/server/tables"
)

var (
	autoMigrateDB = flag.Bool("auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
)

type Database struct {
	GormDB *gorm.DB
}

func NewDatabase(dialect string, args ...interface{}) (*Database, error) {
	gdb, err := gorm.Open(dialect, args...)
	if err != nil {
		return nil, err
	}
	gdb.SingularTable(true)
	gdb.LogMode(true)
	if *autoMigrateDB {
		gdb.AutoMigrate(tables.GetAllTables()...)
	}
	return &Database{
		GormDB: gdb,
	}, nil
}

func GetConfiguredDatabase(c *config.Configurator) (*Database, error) {
	datasource := c.GetDBDataSource()
	if datasource != "" {
		parts := strings.SplitN(datasource, "://", 2)
		dialect, connString := parts[0], parts[1]
		return NewDatabase(dialect, connString)
	}
	return nil, fmt.Errorf("No database configured -- please specify at least one in the config")
}

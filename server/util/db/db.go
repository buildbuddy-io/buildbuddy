package db

import (
	"flag"
	"fmt"
	"log"
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

	readReplicaDB *gorm.DB
}

type Options interface {
	ReadOnly() bool
	AllowStaleReads() bool
}

type optionsImpl struct {
	readOnly        bool
	allowStaleReads bool
}

func (oi *optionsImpl) ReadOnly() bool        { return oi.readOnly }
func (oi *optionsImpl) AllowStaleReads() bool { return oi.allowStaleReads }

type txRunner func(tx *gorm.DB) error

func StaleReadOptions() Options {
	return &optionsImpl{
		readOnly:        true,
		allowStaleReads: true,
	}
}

func ReadWriteOptions() Options {
	return &optionsImpl{
		readOnly:        false,
		allowStaleReads: false,
	}
}

func (dbh *DBHandle) TransactionWithOptions(opts Options, txn txRunner) error {
	if opts.ReadOnly() && opts.AllowStaleReads() {
		if dbh.readReplicaDB != nil {
			return dbh.readReplicaDB.Transaction(txn)
		}
	}
	return dbh.DB.Transaction(txn)
}

func (dbh *DBHandle) Transaction(txn txRunner) error {
	return dbh.DB.Transaction(txn)
}

func maybeRunMigrations(dialect string, gdb *gorm.DB) error {
	if *autoMigrateDB {
		postAutoMigrateFuncs, err := tables.PreAutoMigrate(gdb)
		if err != nil {
			return err
		}
		gdb.AutoMigrate(tables.GetAllTables()...)
		if postAutoMigrateFuncs != nil {
			for _, f := range postAutoMigrateFuncs {
				if err := f(); err != nil {
					return err
				}
			}
		}
		if err := tables.PostAutoMigrate(gdb); err != nil {
			return err
		}
	}
	// SQLITE Special! To avoid "database is locked errors":
	if dialect == sqliteDialect {
		gdb.DB().SetMaxOpenConns(1)
		gdb.Exec("PRAGMA journal_mode=WAL;")
	}
	return nil
}

func openDB(dialect string, args ...interface{}) (*gorm.DB, error) {
	gdb, err := gorm.Open(dialect, args...)
	if err != nil {
		return nil, err
	}
	gdb.SingularTable(true)
	gdb.LogMode(false)
	return gdb, nil
}

func parseDatasource(datasource string) (string, string, error) {
	if datasource != "" {
		parts := strings.SplitN(datasource, "://", 2)
		if len(parts) != 2 {
			return "", "", fmt.Errorf("malformed db connection string")
		}
		dialect, connString := parts[0], parts[1]
		return dialect, connString, nil
	}
	return "", "", fmt.Errorf("No database configured -- please specify at least one in the config")
}

func GetConfiguredDatabase(c *config.Configurator) (*DBHandle, error) {
	if c.GetDBDataSource() == "" {
		return nil, fmt.Errorf("No database configured -- please specify one in the config")
	}
	dialect, connString, err := parseDatasource(c.GetDBDataSource())
	if err != nil {
		return nil, err
	}
	primaryDB, err := openDB(dialect, connString)
	if err != nil {
		return nil, err
	}
	if err := maybeRunMigrations(dialect, primaryDB); err != nil {
		return nil, err
	}

	dbh := &DBHandle{
		DB:      primaryDB,
		dialect: dialect,
	}

	// Setup a read replica if one is configured.
	if c.GetDBReadReplica() != "" {
		readDialect, readConnString, err := parseDatasource(c.GetDBReadReplica())
		if err != nil {
			return nil, err
		}
		replicaDB, err := openDB(readDialect, readConnString)
		if err != nil {
			return nil, err
		}
		log.Print("Read replica was present -- connecting to it.")
		dbh.readReplicaDB = replicaDB
	}
	return dbh, nil
}

func (h *DBHandle) DateFromUsecTimestamp(fieldName string) string {
	if h.dialect == sqliteDialect {
		return "DATE(" + fieldName + "/1000000, 'unixepoch')"
	}
	return "DATE(FROM_UNIXTIME(" + fieldName + "/1000000))"
}

package db

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/prometheus/client_golang/prometheus"

	// We support MySQL (preferred), Postgresql, and Sqlite3
	_ "github.com/jinzhu/gorm/dialects/mysql"
	_ "github.com/jinzhu/gorm/dialects/postgres"
	_ "github.com/jinzhu/gorm/dialects/sqlite"

	// Allow for "cloudsql" type connections that support workload identity.
	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
)

const (
	sqliteDialect              = "sqlite3"
	defaultDbStatsPollInterval = 5 * time.Second
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
	return nil
}

func openDB(dialect string, args ...interface{}) (*gorm.DB, error) {
	gdb, err := gorm.Open(dialect, args...)
	if err != nil {
		return nil, err
	}
	gdb.SingularTable(true)
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

func setDBOptions(c *config.Configurator, dialect string, gdb *gorm.DB) {
	// SQLITE Special! To avoid "database is locked errors":
	if dialect == sqliteDialect {
		gdb.DB().SetMaxOpenConns(1)
		gdb.Exec("PRAGMA journal_mode=WAL;")
	} else {
		if maxOpenConns := c.GetDatabaseConfig().MaxOpenConns; maxOpenConns != 0 {
			gdb.DB().SetMaxOpenConns(maxOpenConns)
		}
		if maxIdleConns := c.GetDatabaseConfig().MaxIdleConns; maxIdleConns != 0 {
			gdb.DB().SetMaxIdleConns(maxIdleConns)
		}
		if connMaxLifetimeSecs := c.GetDatabaseConfig().ConnMaxLifetimeSeconds; connMaxLifetimeSecs != 0 {
			gdb.DB().SetConnMaxLifetime(time.Duration(connMaxLifetimeSecs) * time.Second)
		}
	}
	gdb.LogMode(c.GetDatabaseConfig().LogQueries)
}

type dbStatsRecorder struct {
	db                *sql.DB
	role              string
	lastRecordedStats sql.DBStats
}

func (r *dbStatsRecorder) poll(interval time.Duration) {
	for {
		r.recordStats()
		time.Sleep(interval)
	}
}

func (r *dbStatsRecorder) recordStats() {
	stats := r.db.Stats()

	roleLabel := prometheus.Labels{
		metrics.SQLDBRoleLabel: r.role,
	}

	metrics.SQLMaxOpenConnections.With(roleLabel).Set(float64(stats.MaxOpenConnections))
	metrics.SQLOpenConnections.With(prometheus.Labels{
		metrics.SQLConnectionStatusLabel: "in_use",
		metrics.SQLDBRoleLabel:           r.role,
	}).Set(float64(stats.InUse))
	metrics.SQLOpenConnections.With(prometheus.Labels{
		metrics.SQLConnectionStatusLabel: "idle",
		metrics.SQLDBRoleLabel:           r.role,
	}).Set(float64(stats.Idle))

	// The following DBStats fields are already counters, so we have
	// to subtract from the last observed value to know how much to
	// increment by.
	last := r.lastRecordedStats

	metrics.SQLWaitCount.With(roleLabel).Add(float64(stats.WaitCount - last.WaitCount))
	metrics.SQLWaitDuration.With(roleLabel).Add(float64(stats.WaitDuration-last.WaitDuration) / float64(time.Microsecond))
	metrics.SQLMaxIdleClosed.With(roleLabel).Add(float64(stats.MaxIdleClosed - last.MaxIdleClosed))
	metrics.SQLMaxIdleTimeClosed.With(roleLabel).Add(float64(stats.MaxIdleTimeClosed - last.MaxIdleTimeClosed))
	metrics.SQLMaxLifetimeClosed.With(roleLabel).Add(float64(stats.MaxLifetimeClosed - last.MaxLifetimeClosed))

	r.lastRecordedStats = stats
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
	setDBOptions(c, dialect, primaryDB)

	statsPollInterval := defaultDbStatsPollInterval
	dbConf := c.GetDatabaseConfig()
	if dbConf != nil && dbConf.StatsPollInterval != "" {
		if statsPollInterval, err = time.ParseDuration(dbConf.StatsPollInterval); err != nil {
			return nil, err
		}
	}

	statsRecorder := &dbStatsRecorder{
		db:   primaryDB.DB(),
		role: "primary",
	}
	go statsRecorder.poll(statsPollInterval)

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
		setDBOptions(c, readDialect, replicaDB)
		log.Print("Read replica was present -- connecting to it.")
		dbh.readReplicaDB = replicaDB

		statsRecorder := &dbStatsRecorder{
			db:   replicaDB.DB(),
			role: "read_replica",
		}
		go statsRecorder.poll(statsPollInterval)
	}
	return dbh, nil
}

func (h *DBHandle) DateFromUsecTimestamp(fieldName string) string {
	if h.dialect == sqliteDialect {
		return "DATE(" + fieldName + "/1000000, 'unixepoch')"
	}
	return "DATE(FROM_UNIXTIME(" + fieldName + "/1000000))"
}

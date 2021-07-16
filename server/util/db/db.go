package db

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	golog "log"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	// We support MySQL (preferred), Postgresql, and Sqlite3.
	// New dialects need to be added to openDB() as well.
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"

	// Allow for "cloudsql" type connections that support workload identity.
	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	sqliteDialect   = "sqlite3"
	mysqlDialect    = "mysql"
	postgresDialect = "postgres"

	defaultDbStatsPollInterval       = 5 * time.Second
	gormStmtStartTimeKey             = "buildbuddy:op_start_time"
	gormRecordOpStartTimeCallbackKey = "buildbuddy:record_op_start_time"
	gormRecordMetricsCallbackKey     = "buildbuddy:record_metrics"
)

var (
	autoMigrateDB        = flag.Bool("auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
	autoMigrateDBAndExit = flag.Bool("auto_migrate_db_and_exit", false, "If true, attempt to automigrate the db when connecting, then exit the program.")
)

type DBHandle struct {
	*gorm.DB
	readReplicaDB *gorm.DB
	dialect       string
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

type DB = gorm.DB

type txRunner func(tx *DB) error

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

func (dbh *DBHandle) TransactionWithOptions(ctx context.Context, opts Options, txn txRunner) error {
	if opts.ReadOnly() && opts.AllowStaleReads() {
		if dbh.readReplicaDB != nil {
			return dbh.readReplicaDB.WithContext(ctx).Transaction(txn)
		}
	}
	return dbh.DB.WithContext(ctx).Transaction(txn)
}

func (dbh *DBHandle) Transaction(ctx context.Context, txn txRunner) error {
	return dbh.DB.WithContext(ctx).Transaction(txn)
}

func IsRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

func (dbh *DBHandle) ReadRow(out interface{}, where ...interface{}) error {
	whereArgs := make([]interface{}, 0)
	if len(where) > 1 {
		whereArgs = where[1:]
	}
	err := dbh.DB.Where(where[0], whereArgs).First(out).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return status.NotFoundError("Record not found")
	}
	return err
}

func runMigrations(dialect string, gdb *gorm.DB) error {
	log.Info("Auto-migrating DB")
	postAutoMigrateFuncs, err := tables.PreAutoMigrate(gdb)
	if err != nil {
		return err
	}
	gdb.AutoMigrate(tables.GetAllTables()...)
	for _, f := range postAutoMigrateFuncs {
		if err := f(); err != nil {
			return err
		}
	}
	if err := tables.PostAutoMigrate(gdb); err != nil {
		return err
	}
	return nil
}

// instrumentGORM adds GORM callbacks that populate query metrics.
func instrumentGORM(gdb *gorm.DB) {
	// Add callback that runs before other callbacks that records when the operation began.
	// We use this to calculate how long a query takes to run.
	recordStartTime := func(db *gorm.DB) {
		if db.DryRun || db.Statement == nil {
			return
		}
		db.Statement.Settings.Store(gormStmtStartTimeKey, time.Now())
	}
	gdb.Callback().Create().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordStartTime)
	gdb.Callback().Delete().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordStartTime)
	gdb.Callback().Query().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordStartTime)
	gdb.Callback().Raw().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordStartTime)
	gdb.Callback().Row().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordStartTime)
	gdb.Callback().Update().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordStartTime)

	// Add callback that runs after other callbacks that records executed queries and their durations.
	recordMetrics := func(db *gorm.DB) {
		if db.DryRun || db.Statement == nil {
			return
		}
		labels := prometheus.Labels{
			metrics.SQLQueryTemplateLabel: db.Statement.SQL.String(),
		}
		metrics.SQLQueryCount.With(labels).Inc()
		// v will be nil if our key is not in the map so we can ignore the presence indicator.
		v, _ := db.Statement.Settings.LoadAndDelete(gormStmtStartTimeKey)
		if opStartTime, ok := v.(time.Time); ok {
			metrics.SQLQueryDurationUsec.With(labels).Observe(float64(time.Now().Sub(opStartTime).Microseconds()))
		}
		// Ignore "record not found" errors as they don't generally indicate a
		// problem with the server.
		if db.Error != nil && !errors.Is(db.Error, gorm.ErrRecordNotFound) {
			metrics.SQLErrorCount.With(labels).Inc()
		}
	}
	gdb.Callback().Create().After("*").Register(gormRecordMetricsCallbackKey, recordMetrics)
	gdb.Callback().Delete().After("*").Register(gormRecordMetricsCallbackKey, recordMetrics)
	gdb.Callback().Query().After("*").Register(gormRecordMetricsCallbackKey, recordMetrics)
	gdb.Callback().Raw().After("*").Register(gormRecordMetricsCallbackKey, recordMetrics)
	gdb.Callback().Row().After("*").Register(gormRecordMetricsCallbackKey, recordMetrics)
	gdb.Callback().Update().After("*").Register(gormRecordMetricsCallbackKey, recordMetrics)
}

func openDB(configurator *config.Configurator, dialect string, connString string) (*gorm.DB, error) {
	var dialector gorm.Dialector
	switch dialect {
	case sqliteDialect:
		dialector = sqlite.Open(connString)
	case mysqlDialect:
		// Set default string size to 255 to avoid unnecessary schema modifications by GORM.
		// Newer versions of GORM use a smaller default size (191) to account for InnoDB index limits
		// that don't apply to modern MysQL installations.
		dialector = mysql.New(mysql.Config{DSN: connString, DefaultStringSize: 255})
	case postgresDialect:
		dialector = postgres.Open(connString)
	default:
		return nil, fmt.Errorf("unsupported database dialect %s", dialect)
	}

	var l logger.Interface
	// This is the same as logger.Default, but with colors turned off (since the
	// output may not support colors) and sending output to stderr (to be
	// consistent with the rest of our logs).
	gormLogger := logger.New(
		golog.New(os.Stderr, "\r\n", golog.LstdFlags),
		logger.Config{
			SlowThreshold: 200 * time.Millisecond,
			LogLevel:      logger.Warn,
			Colorful:      false,
		})
	l = sqlLogger{Interface: gormLogger, logLevel: logger.Warn}
	if configurator.GetDatabaseConfig().LogQueries {
		l = l.LogMode(logger.Info)
	}
	config := gorm.Config{
		Logger: l,
	}
	gdb, err := gorm.Open(dialector, &config)
	if err != nil {
		return nil, err
	}

	instrumentGORM(gdb)

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

// sqlLogger is a GORM logger wrapper that supresses "record not found" errors.
type sqlLogger struct {
	logger.Interface
	logLevel logger.LogLevel
}

func (l sqlLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	// Avoid logging errors when no records are matched for a lookup as it
	// generally does not indicate a problem with the server.
	// Except when log level is "info" where we log all queries.
	if err != nil && errors.Is(err, gorm.ErrRecordNotFound) && l.logLevel != logger.Info {
		return
	}
	l.Interface.Trace(ctx, begin, fc, err)
}
func (l sqlLogger) LogMode(level logger.LogLevel) logger.Interface {
	return sqlLogger{l.Interface.LogMode(level), level}
}

func setDBOptions(c *config.Configurator, dialect string, gdb *gorm.DB) error {
	db, err := gdb.DB()
	if err != nil {
		return err
	}

	// SQLITE Special! To avoid "database is locked errors":
	if dialect == sqliteDialect {
		db.SetMaxOpenConns(1)
		gdb.Exec("PRAGMA journal_mode=WAL;")
	} else {
		if maxOpenConns := c.GetDatabaseConfig().MaxOpenConns; maxOpenConns != 0 {
			db.SetMaxOpenConns(maxOpenConns)
		}
		if maxIdleConns := c.GetDatabaseConfig().MaxIdleConns; maxIdleConns != 0 {
			db.SetMaxIdleConns(maxIdleConns)
		}
		if connMaxLifetimeSecs := c.GetDatabaseConfig().ConnMaxLifetimeSeconds; connMaxLifetimeSecs != 0 {
			db.SetConnMaxLifetime(time.Duration(connMaxLifetimeSecs) * time.Second)
		}
	}

	return nil
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

func GetConfiguredDatabase(c *config.Configurator, hc interfaces.HealthChecker) (*DBHandle, error) {
	if c.GetDBDataSource() == "" {
		return nil, fmt.Errorf("No database configured -- please specify one in the config")
	}
	dialect, connString, err := parseDatasource(c.GetDBDataSource())
	if err != nil {
		return nil, err
	}
	primaryDB, err := openDB(c, dialect, connString)
	if err != nil {
		return nil, err
	}

	err = setDBOptions(c, dialect, primaryDB)
	if err != nil {
		return nil, err
	}

	statsPollInterval := defaultDbStatsPollInterval
	dbConf := c.GetDatabaseConfig()
	if dbConf != nil && dbConf.StatsPollInterval != "" {
		if statsPollInterval, err = time.ParseDuration(dbConf.StatsPollInterval); err != nil {
			return nil, err
		}
	}

	primarySQLDB, err := primaryDB.DB()
	if err != nil {
		return nil, err
	}
	statsRecorder := &dbStatsRecorder{
		db:   primarySQLDB,
		role: "primary",
	}
	go statsRecorder.poll(statsPollInterval)

	if *autoMigrateDBAndExit {
		if err := runMigrations(dialect, primaryDB); err != nil {
			log.Fatalf("Database auto-migration failed: %s", err)
		}
		log.Infof("Database migration completed. Exiting due to --auto_migrate_db_and_exit.")
		os.Exit(0)
	}
	if *autoMigrateDB {
		if err := runMigrations(dialect, primaryDB); err != nil {
			return nil, err
		}
	}

	dbh := &DBHandle{
		DB:      primaryDB,
		dialect: dialect,
	}
	hc.AddHealthCheck("sql_primary", interfaces.CheckerFunc(func(ctx context.Context) error {
		return primarySQLDB.Ping()
	}))

	// Setup a read replica if one is configured.
	if c.GetDBReadReplica() != "" {
		readDialect, readConnString, err := parseDatasource(c.GetDBReadReplica())
		if err != nil {
			return nil, err
		}
		replicaDB, err := openDB(c, readDialect, readConnString)
		if err != nil {
			return nil, err
		}
		setDBOptions(c, readDialect, replicaDB)
		log.Info("Read replica was present -- connecting to it.")
		dbh.readReplicaDB = replicaDB

		replicaSQLDB, err := replicaDB.DB()
		if err != nil {
			return nil, err
		}
		statsRecorder := &dbStatsRecorder{
			db:   replicaSQLDB,
			role: "read_replica",
		}
		go statsRecorder.poll(statsPollInterval)

		hc.AddHealthCheck("sql_read_replica", interfaces.CheckerFunc(func(ctx context.Context) error {
			return replicaSQLDB.Ping()
		}))
	}
	return dbh, nil
}

func (h *DBHandle) DateFromUsecTimestamp(fieldName string) string {
	if h.dialect == sqliteDialect {
		return "DATE(" + fieldName + "/1000000, 'unixepoch')"
	}
	return "DATE(FROM_UNIXTIME(" + fieldName + "/1000000))"
}

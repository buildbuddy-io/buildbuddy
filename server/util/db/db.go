package db

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	golog "log"

	"github.com/aws/aws-sdk-go/service/rds/rdsutils"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	// We support MySQL (preferred) and Sqlite3.
	// New dialects need to be added to openDB() as well.
	"gorm.io/driver/mysql"
	"gorm.io/driver/sqlite"

	// Allow for "cloudsql" type connections that support workload identity.
	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"

	awssession "github.com/aws/aws-sdk-go/aws/session"
	gomysql "github.com/go-sql-driver/mysql"
	gosqlite "github.com/mattn/go-sqlite3"
)

const (
	sqliteDriver = "sqlite3"
	mysqlDriver  = "mysql"

	gormStmtStartTimeKey             = "buildbuddy:op_start_time"
	gormRecordOpStartTimeCallbackKey = "buildbuddy:record_op_start_time"
	gormRecordMetricsCallbackKey     = "buildbuddy:record_metrics"
	gormQueryNameKey                 = "buildbuddy:query_name"

	gormStmtSpanKey          = "buildbuddy:span"
	gormStartSpanCallbackKey = "buildbuddy:start_span"
	gormEndSpanCallbackKey   = "buildbuddy:end_span"
)

var (
	dataSource             = flagutil.New("database.data_source", "sqlite3:///tmp/buildbuddy.db", "The SQL database to connect to, specified as a connection string.", flagutil.SecretTag)
	advDataSource          = flagutil.New("database.advanced_data_source", AdvancedConfig{}, "Alternative to the database.data_source flag that allows finer control over database settings as well as allowing use of AWS IAM credentials. For most users, database.data_source is a simpler configuration method.")
	readReplica            = flag.String("database.read_replica", "", "A secondary, read-only SQL database to connect to, specified as a connection string.")
	advReadReplica         = flagutil.New("database.advanced_read_replica", AdvancedConfig{}, "Advanced alternative to database.read_replica. Refer to database.advanced for more information.")
	statsPollInterval      = flag.Duration("database.stats_poll_interval", 5*time.Second, "How often to poll the DB client for connection stats (default: '5s').")
	maxOpenConns           = flag.Int("database.max_open_conns", 0, "The maximum number of open connections to maintain to the db")
	maxIdleConns           = flag.Int("database.max_idle_conns", 0, "The maximum number of idle connections to maintain to the db")
	connMaxLifetimeSeconds = flag.Int("database.conn_max_lifetime_seconds", 0, "The maximum lifetime of a connection to the db")
	logQueries             = flag.Bool("database.log_queries", false, "If true, log all queries")
	slowQueryThreshold     = flag.Duration("database.slow_query_threshold", 500*time.Millisecond, "Queries longer than this duration will be logged with a 'Slow SQL' warning.")

	autoMigrateDB        = flag.Bool("auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
	autoMigrateDBAndExit = flag.Bool("auto_migrate_db_and_exit", false, "If true, attempt to automigrate the db when connecting, then exit the program.")
)

type AdvancedConfig struct {
	Driver    string `yaml:"driver" usage:"The driver to use: one of sqlite3 or mysql."`
	Endpoint  string `yaml:"endpoint" usage:"Typically the host:port combination of the database server."`
	Username  string `yaml:"username" usage:"Username to use when connecting."`
	Password  string `yaml:"password" usage:"Password to use when connecting. Not used if AWS IAM is enabled."`
	DBName    string `yaml:"db_name" usage:"The name of the database to use for BuildBuddy data."`
	Region    string `yaml:"region" usage:"Region of the database instance. Required if AWS IAM is enabled."`
	UseAWSIAM bool   `yaml:"use_aws_iam" usage:"If enabled, AWS IAM authentication is used instead of fixed credentials. Make sure the endpoint includes the port, otherwise IAM-based auth will fail."`
	Params    string `yaml:"params" usage:"Optional parameters to pass to the database driver (in format key1=val1&key2=val2)"`
}

// dnsFormatter generates DSN strings from structured options.
type dsnFormatter struct {
	Driver   string
	Endpoint string
	Username string
	Password string
	DBName   string
	Params   string
}

func (df *dsnFormatter) AddParam(key, val string) {
	if df.Params != "" {
		df.Params += "&"
	}
	df.Params += fmt.Sprintf("%s=%s", key, val)
}

func (df *dsnFormatter) String() string {
	endpoint := df.Endpoint
	if df.Driver == mysqlDriver {
		endpoint = fmt.Sprintf("tcp(%s)", df.Endpoint)
	}

	b := strings.Builder{}
	if df.Username != "" && df.Password != "" {
		b.WriteString(df.Username)
		b.WriteString(":")
		b.WriteString(df.Password)
		b.WriteString("@")
	}
	b.WriteString(endpoint)
	if df.DBName != "" {
		b.WriteString("/")
		b.WriteString(df.DBName)
	}
	if df.Params != "" {
		b.WriteString("?")
		b.WriteString(df.Params)
	}
	return b.String()
}

type DBHandle struct {
	db            *gorm.DB
	readReplicaDB *gorm.DB
	driver        string
}

type Options struct {
	readOnly        bool
	allowStaleReads bool
	queryName       string
}

func Opts() interfaces.DBOptions {
	return &Options{}
}

func (o *Options) WithStaleReads() interfaces.DBOptions {
	o.readOnly = true
	o.allowStaleReads = true
	return o
}

// WithQueryName specifies the query label to use in exported metrics.
func (o *Options) WithQueryName(queryName string) interfaces.DBOptions {
	o.queryName = queryName
	return o
}

func (o *Options) ReadOnly() bool {
	return o.readOnly
}

func (o *Options) AllowStaleReads() bool {
	return o.allowStaleReads
}

func (o *Options) QueryName() string {
	return o.queryName
}

type DB = gorm.DB

func (dbh *DBHandle) DB(ctx context.Context) *DB {
	return dbh.db.WithContext(ctx)
}

func (dbh *DBHandle) gormHandleForOpts(ctx context.Context, opts interfaces.DBOptions) *DB {
	db := dbh.DB(ctx)
	if opts.ReadOnly() && opts.AllowStaleReads() && dbh.readReplicaDB != nil {
		db = dbh.readReplicaDB
	}

	if opts.QueryName() != "" {
		db = db.Set(gormQueryNameKey, opts.QueryName())
	}
	return db
}

func (dbh *DBHandle) RawWithOptions(ctx context.Context, opts interfaces.DBOptions, sql string, values ...interface{}) *gorm.DB {
	return dbh.gormHandleForOpts(ctx, opts).Raw(sql, values...)
}

func (dbh *DBHandle) TransactionWithOptions(ctx context.Context, opts interfaces.DBOptions, txn interfaces.TxRunner) error {
	return dbh.gormHandleForOpts(ctx, opts).Transaction(txn)
}

func (dbh *DBHandle) Transaction(ctx context.Context, txn interfaces.TxRunner) error {
	return dbh.DB(ctx).Transaction(txn)
}

func IsRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
}

func (dbh *DBHandle) ReadRow(ctx context.Context, out interface{}, where ...interface{}) error {
	whereArgs := make([]interface{}, 0)
	if len(where) > 1 {
		whereArgs = where[1:]
	}
	err := dbh.DB(ctx).Where(where[0], whereArgs).First(out).Error
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
	if err := gdb.AutoMigrate(tables.GetAllTables()...); err != nil {
		return err
	}
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

func makeStartSpanBeforeFn(spanName string) func(db *gorm.DB) {
	return func(db *gorm.DB) {
		ctx, span := tracing.StartSpan(db.Statement.Context)
		span.SetName(spanName)
		db.Statement.Context = ctx
		db.Statement.Settings.Store(gormStmtSpanKey, span)
	}
}

func recordMetricstBeforeFn(db *gorm.DB) {
	if db.DryRun || db.Statement == nil {
		return
	}
	db.Statement.Settings.Store(gormStmtStartTimeKey, time.Now())
}

func recordMetricsAfterFn(db *gorm.DB) {
	if db.DryRun || db.Statement == nil {
		return
	}

	labels := prometheus.Labels{}
	qv, _ := db.Get(gormQueryNameKey)
	if queryName, ok := qv.(string); ok {
		labels[metrics.SQLQueryTemplateLabel] = queryName
	} else {
		labels[metrics.SQLQueryTemplateLabel] = db.Statement.SQL.String()
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

func recordSpanAfterFn(db *gorm.DB) {
	// v will be nil if our key is not in the map so we can ignore the presence indicator.
	spanIface, _ := db.Statement.Settings.LoadAndDelete(gormStmtSpanKey)
	span, ok := spanIface.(trace.Span)
	if !ok {
		return
	}
	if !span.IsRecording() {
		return
	}
	attrs := []attribute.KeyValue{
		attribute.String("dialect", db.Dialector.Name()),
		attribute.String("query", db.Statement.SQL.String()),
		attribute.String("table", db.Statement.Table),
		attribute.Int64("rows_affected", db.Statement.RowsAffected),
	}
	span.SetAttributes(attrs...)
	if db.Error != nil {
		span.RecordError(db.Error)
		span.SetStatus(codes.Error, db.Error.Error())
	}
	span.End()
}

// instrumentGORM adds GORM callbacks that populate query metrics.
func instrumentGORM(gdb *gorm.DB) {
	// Add callback that runs before other callbacks that records when the operation began.
	// We use this to calculate how long a query takes to run.
	gdb.Callback().Create().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordMetricstBeforeFn)
	gdb.Callback().Delete().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordMetricstBeforeFn)
	gdb.Callback().Query().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordMetricstBeforeFn)
	gdb.Callback().Raw().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordMetricstBeforeFn)
	gdb.Callback().Row().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordMetricstBeforeFn)
	gdb.Callback().Update().Before("*").Register(gormRecordOpStartTimeCallbackKey, recordMetricstBeforeFn)

	gdb.Callback().Create().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:create"))
	gdb.Callback().Delete().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:delete"))
	gdb.Callback().Query().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:query"))
	gdb.Callback().Raw().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:raw"))
	gdb.Callback().Row().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:row"))
	gdb.Callback().Update().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:update"))

	// Add callback that runs after other callbacks that records executed queries and their durations.
	gdb.Callback().Create().After("*").Register(gormRecordMetricsCallbackKey, recordMetricsAfterFn)
	gdb.Callback().Delete().After("*").Register(gormRecordMetricsCallbackKey, recordMetricsAfterFn)
	gdb.Callback().Query().After("*").Register(gormRecordMetricsCallbackKey, recordMetricsAfterFn)
	gdb.Callback().Raw().After("*").Register(gormRecordMetricsCallbackKey, recordMetricsAfterFn)
	gdb.Callback().Row().After("*").Register(gormRecordMetricsCallbackKey, recordMetricsAfterFn)
	gdb.Callback().Update().After("*").Register(gormRecordMetricsCallbackKey, recordMetricsAfterFn)

	gdb.Callback().Create().After("*").Register(gormEndSpanCallbackKey, recordSpanAfterFn)
	gdb.Callback().Delete().After("*").Register(gormEndSpanCallbackKey, recordSpanAfterFn)
	gdb.Callback().Query().After("*").Register(gormEndSpanCallbackKey, recordSpanAfterFn)
	gdb.Callback().Raw().After("*").Register(gormEndSpanCallbackKey, recordSpanAfterFn)
	gdb.Callback().Row().After("*").Register(gormEndSpanCallbackKey, recordSpanAfterFn)
	gdb.Callback().Update().After("*").Register(gormEndSpanCallbackKey, recordSpanAfterFn)
}

// connector implements the sql Driver interface which allows us to control the
// DSN passed to the driver for every new connection instead of using a single
// fixed DSN.
//
// Some connectivity (i.e. AWS IAM) uses short-lived tokens which requires us
// to update the DSN. We delegate to the DataSource to generate the DSN.
type connector struct {
	d  driver.Driver
	ds DataSource
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	dsn, err := c.ds.DSN()
	if err != nil {
		return nil, status.UnavailableErrorf("could not generate DSN: %s", err)
	}
	return c.d.Open(dsn)
}

func (c *connector) Driver() driver.Driver {
	return c.d
}

func openDB(dataSource string, advancedConfig *AdvancedConfig) (*gorm.DB, string, error) {
	ds, err := ParseDatasource(dataSource, advancedConfig)
	if err != nil {
		return nil, "", err
	}

	var drv driver.Driver
	switch ds.DriverName() {
	case sqliteDriver:
		drv = &gosqlite.SQLiteDriver{}
	case mysqlDriver:
		drv = &gomysql.MySQLDriver{}
	default:
		return nil, "", fmt.Errorf("unsupported database driver %s", ds.DriverName())
	}

	// Use our own connector so that we can control the DSN for each new
	// connection.
	db := sql.OpenDB(&connector{d: drv, ds: ds})

	var dialector gorm.Dialector
	switch ds.DriverName() {
	case sqliteDriver:
		dialector = sqlite.Dialector{Conn: db}
	case mysqlDriver:
		// Set default string size to 255 to avoid unnecessary schema modifications by GORM.
		// Newer versions of GORM use a smaller default size (191) to account for InnoDB index limits
		// that don't apply to modern MysQL installations.
		dialector = mysql.New(mysql.Config{Conn: db, DefaultStringSize: 255})
	default:
		return nil, "", fmt.Errorf("unsupported database driver %s", ds.DriverName())
	}

	var l logger.Interface
	// This is the same as logger.Default, but with colors turned off (since the
	// output may not support colors) and sending output to stderr (to be
	// consistent with the rest of our logs).
	// TODO: Have all logs written to zerolog instead.
	gormLogger := logger.New(
		golog.New(os.Stderr, "\r\n", golog.LstdFlags),
		logger.Config{
			SlowThreshold: *slowQueryThreshold,
			LogLevel:      logger.Warn,
			// Disable log colors when structured logging is enabled.
			Colorful: *log.EnableStructuredLogging,
		})
	l = sqlLogger{Interface: gormLogger, logLevel: logger.Warn}
	if *logQueries {
		l = l.LogMode(logger.Info)
	}
	config := gorm.Config{
		Logger: l,
	}
	gdb, err := gorm.Open(dialector, &config)
	if err != nil {
		return nil, "", err
	}

	instrumentGORM(gdb)

	return gdb, ds.DriverName(), nil
}

// DataSource is responsible for generating the DSN for new sql connections.
type DataSource interface {
	DriverName() string
	DSN() (string, error)
}

// fixedDSNDataSource returns a fixed DSN.
type fixedDSNDataSource struct {
	driver string
	dsn    string
}

func (fd *fixedDSNDataSource) DriverName() string {
	return fd.driver
}

func (fd *fixedDSNDataSource) DSN() (string, error) {
	return fd.dsn, nil
}

// awsIAMDataSource generates the DSN using short-lived AWS IAM auth tokens.
type awsIAMDataSource struct {
	baseDSN *dsnFormatter
	region  string
	session *awssession.Session
}

func (aid *awsIAMDataSource) DriverName() string {
	return aid.baseDSN.Driver
}

func (aid *awsIAMDataSource) DSN() (string, error) {
	creds := aid.session.Config.Credentials
	token, err := rdsutils.BuildAuthToken(aid.baseDSN.Endpoint, aid.region, aid.baseDSN.Username, creds)
	if err != nil {
		return "", status.UnavailableErrorf("could not obtain AWS IAM auth token: %s", err)
	}
	dsn := *aid.baseDSN
	dsn.Password = token
	return dsn.String(), nil
}

func ParseDatasource(datasource string, advancedConfig *AdvancedConfig) (DataSource, error) {
	if *advancedConfig != (AdvancedConfig{}) {
		ac := advancedConfig
		dsn := &dsnFormatter{
			Driver:   ac.Driver,
			Endpoint: ac.Endpoint,
			Username: ac.Username,
			Password: ac.Password,
			DBName:   ac.DBName,
			Params:   ac.Params,
		}

		if ac.Endpoint == "" {
			return nil, status.FailedPreconditionError("endpoint is required")
		}

		if ac.UseAWSIAM {
			if ac.Region == "" {
				return nil, status.FailedPreconditionError("region is required to enable AWS IAM")
			}
			if ac.Password != "" {
				return nil, status.FailedPreconditionError("password should not be specified when AWS IAM is enabled")
			}

			if ac.Driver == mysqlDriver {
				certPool := x509.NewCertPool()
				// This file is packaged in the enterprise docker image.
				pem, err := os.ReadFile("/rds-combined-ca-bundle.pem")
				// This file is packaged in the enterprise docker image.
				if err != nil {
					return nil, status.UnavailableErrorf("could not read RDS CA bundle: %s", err)
				}
				if !certPool.AppendCertsFromPEM(pem) {
					return nil, status.UnavailableErrorf("could not parse RDS CA bundle")
				}
				if err = gomysql.RegisterTLSConfig("rds", &tls.Config{RootCAs: certPool}); err != nil {
					return nil, status.UnknownErrorf("could not configure RDS CA bundle for mysql: %s", err)
				}
				dsn.AddParam("tls", "rds")
				dsn.AddParam("allowCleartextPasswords", "true")
			}

			sess, err := awssession.NewSession()
			if err != nil {
				return nil, status.FailedPreconditionErrorf("could not initialize AWS session: %s", err)
			}
			return &awsIAMDataSource{
				baseDSN: dsn,
				region:  ac.Region,
				session: sess,
			}, nil
		}

		return &fixedDSNDataSource{driver: ac.Driver, dsn: dsn.String()}, nil
	}

	if datasource != "" {
		parts := strings.SplitN(datasource, "://", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed db connection string")
		}
		driverName, connString := parts[0], parts[1]
		return &fixedDSNDataSource{driver: driverName, dsn: connString}, nil
	}

	return nil, status.FailedPreconditionError("no database configured -- please specify at least one in the config")
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

func setDBOptions(driver string, gdb *gorm.DB) error {
	db, err := gdb.DB()
	if err != nil {
		return err
	}

	// SQLITE Special! To avoid "database is locked errors":
	if driver == sqliteDriver {
		db.SetMaxOpenConns(1)
		gdb.Exec("PRAGMA journal_mode=WAL;")
	} else {
		if *maxOpenConns != 0 {
			db.SetMaxOpenConns(*maxOpenConns)
		}
		if *maxIdleConns != 0 {
			db.SetMaxIdleConns(*maxIdleConns)
		}
		if *connMaxLifetimeSeconds != 0 {
			db.SetConnMaxLifetime(time.Duration(*connMaxLifetimeSeconds) * time.Second)
		}
	}

	return nil
}

type dbStatsRecorder struct {
	db                *sql.DB
	role              string
	lastRecordedStats sql.DBStats
}

func (r *dbStatsRecorder) poll() {
	for {
		r.recordStats()
		time.Sleep(*statsPollInterval)
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

func GetConfiguredDatabase(hc interfaces.HealthChecker) (interfaces.DBHandle, error) {
	if *dataSource == "" {
		return nil, fmt.Errorf("No database configured -- please specify one in the config")
	}
	primaryDB, driverName, err := openDB(*dataSource, advDataSource)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("could not configure primary database: %s", err)
	}

	err = setDBOptions(driverName, primaryDB)
	if err != nil {
		return nil, err
	}

	primarySQLDB, err := primaryDB.DB()
	if err != nil {
		return nil, err
	}
	statsRecorder := &dbStatsRecorder{
		db:   primarySQLDB,
		role: "primary",
	}
	go statsRecorder.poll()

	if *autoMigrateDBAndExit {
		if err := runMigrations(driverName, primaryDB); err != nil {
			log.Fatalf("Database auto-migration failed: %s", err)
		}
		log.Infof("Database migration completed. Exiting due to --auto_migrate_db_and_exit.")
		os.Exit(0)
	}
	if *autoMigrateDB {
		if err := runMigrations(driverName, primaryDB); err != nil {
			return nil, err
		}
	}

	dbh := &DBHandle{
		db:     primaryDB,
		driver: driverName,
	}
	hc.AddHealthCheck("sql_primary", interfaces.CheckerFunc(func(ctx context.Context) error {
		return primarySQLDB.Ping()
	}))

	// Setup a read replica if one is configured.
	if *readReplica != "" {
		replicaDB, readDialect, err := openDB(*readReplica, advReadReplica)
		if err != nil {
			return nil, status.FailedPreconditionErrorf("could not configure read replica database: %s", err)
		}
		setDBOptions(readDialect, replicaDB)
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
		go statsRecorder.poll()

		hc.AddHealthCheck("sql_read_replica", interfaces.CheckerFunc(func(ctx context.Context) error {
			return replicaSQLDB.Ping()
		}))
	}
	return dbh, nil
}

// TODO(bduffany): FROM_UNIXTIME uses the SYSTEM time by default which is not
// guaranteed to be UTC. We should either make sure the MySQL `time_zone` is
// UTC, or do an explicit timezone conversion here from `@@session.time_zone`
// to UTC.

// UTCMonthFromUsecTimestamp returns an SQL expression that converts the value
// of the given field from a Unix timestamp (in microseconds since the Unix
// Epoch) to a month in UTC time, formatted as "YYYY-MM".
func (h *DBHandle) UTCMonthFromUsecTimestamp(fieldName string) string {
	timestampExpr := fieldName + `/1000000`
	if h.driver == sqliteDriver {
		return `STRFTIME('%Y-%m', ` + timestampExpr + `, 'unixepoch')`
	}
	return `DATE_FORMAT(FROM_UNIXTIME(` + timestampExpr + `), '%Y-%m')`
}

// DateFromUsecTimestamp returns an SQL expression that converts the value
// of the given field from a Unix timestamp (in microseconds since the Unix
// Epoch) to a date offset by the given UTC offset. The offset is defined
// according to the description here:
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/getTimezoneOffset
func (h *DBHandle) DateFromUsecTimestamp(fieldName string, timezoneOffsetMinutes int32) string {
	offsetUsec := int64(timezoneOffsetMinutes) * 60 * 1e6
	timestampExpr := fmt.Sprintf("(%s + (%d))/1000000", fieldName, -offsetUsec)
	if h.driver == sqliteDriver {
		return fmt.Sprintf("DATE(%s, 'unixepoch')", timestampExpr)
	}
	return fmt.Sprintf("DATE(FROM_UNIXTIME(%s))", timestampExpr)
}

// InsertIgnoreModifier returns SQL that can be placed after the
// INSERT command to ignore duplicate keys when inserting.
//
// Example:
//
//	`INSERT `+db.InsertIgnoreModifier()+` INTO MyTable
//	 (potentially_already_existing_key)
//	 VALUES ("key_value")`
func (h *DBHandle) InsertIgnoreModifier() string {
	if h.driver == sqliteDriver {
		return "OR IGNORE"
	}
	return "IGNORE"
}

// SelectForUpdateModifier returns SQL that can be placed after the
// SELECT command to lock the rows for update on select.
//
// Example:
//
//	`SELECT column FROM MyTable
//	 WHERE id=<some id> `+db.SelectForUpdateModifier()
func (h *DBHandle) SelectForUpdateModifier() string {
	if h.driver == sqliteDriver {
		return ""
	}
	return "FOR UPDATE"
}

func (h *DBHandle) SetNowFunc(now func() time.Time) {
	h.db.Config.NowFunc = now
}

func (h *DBHandle) IsDuplicateKeyError(err error) bool {
	var mysqlErr *gomysql.MySQLError
	// Defined at https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_dup_entry
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
		return true
	}
	var sqliteErr gosqlite.Error
	// Defined at https://www.sqlite.org/rescode.html#constraint_unique
	if errors.As(err, &sqliteErr) && sqliteErr.ExtendedCode == 2067 {
		return true
	}
	return false
}

func (h *DBHandle) IsDeadlockError(err error) bool {
	var mysqlErr *gomysql.MySQLError
	// Defined at https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_lock_deadlock
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1213 {
		return true
	}
	return false
}

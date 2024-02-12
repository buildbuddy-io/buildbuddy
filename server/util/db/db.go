package db

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/aws_rds_certs"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/gormutil"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"

	// We support MySQL (preferred) and Sqlite3.
	// New dialects need to be added to openDB() as well.
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"

	// Allow for "cloudsql" type connections that support workload identity.
	_ "github.com/GoogleCloudPlatform/cloudsql-proxy/proxy/dialers/mysql"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	rdsauth "github.com/aws/aws-sdk-go-v2/feature/rds/auth"
	gomysql "github.com/go-sql-driver/mysql"
	gopostgreserr "github.com/jackc/pgerrcode"
	gopostgresconn "github.com/jackc/pgx/v5/pgconn"
	gormutils "gorm.io/gorm/utils"
)

const (
	sqliteDriver   = "sqlite3"
	mysqlDriver    = "mysql"
	postgresDriver = "postgresql"

	gormStmtStartTimeKey             = "buildbuddy:op_start_time"
	gormRecordOpStartTimeCallbackKey = "buildbuddy:record_op_start_time"
	gormRecordMetricsCallbackKey     = "buildbuddy:record_metrics"
	gormQueryNameKey                 = "buildbuddy:query_name"

	gormStmtSpanKey          = "buildbuddy:span"
	gormStartSpanCallbackKey = "buildbuddy:start_span"
	gormEndSpanCallbackKey   = "buildbuddy:end_span"
)

var (
	dataSource             = flag.String("database.data_source", "sqlite3:///tmp/buildbuddy.db", "The SQL database to connect to, specified as a connection string.", flag.Secret)
	advDataSource          = flag.Struct("database.advanced_data_source", AdvancedConfig{}, "Alternative to the database.data_source flag that allows finer control over database settings as well as allowing use of AWS IAM credentials. For most users, database.data_source is a simpler configuration method.")
	readReplica            = flag.String("database.read_replica", "", "A secondary, read-only SQL database to connect to, specified as a connection string.")
	advReadReplica         = flag.Struct("database.advanced_read_replica", AdvancedConfig{}, "Advanced alternative to database.read_replica. Refer to database.advanced for more information.")
	statsPollInterval      = flag.Duration("database.stats_poll_interval", 5*time.Second, "How often to poll the DB client for connection stats (default: '5s').")
	maxOpenConns           = flag.Int("database.max_open_conns", 0, "The maximum number of open connections to maintain to the db")
	maxIdleConns           = flag.Int("database.max_idle_conns", 0, "The maximum number of idle connections to maintain to the db")
	connMaxLifetimeSeconds = flag.Int("database.conn_max_lifetime_seconds", 0, "The maximum lifetime of a connection to the db")
	logQueries             = flag.Bool("database.log_queries", false, "If true, log all queries")
	slowQueryThreshold     = flag.Duration("database.slow_query_threshold", 500*time.Millisecond, "Queries longer than this duration will be logged with a 'Slow SQL' warning.")

	autoMigrateDB             = flag.Bool("auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
	autoMigrateDBAndExit      = flag.Bool("auto_migrate_db_and_exit", false, "If true, attempt to automigrate the db when connecting, then exit the program.")
	printSchemaChangesAndExit = flag.Bool("database.print_schema_changes_and_exit", false, "If set, print schema changes from auto-migration, then exit the program.")
)

type AdvancedConfig struct {
	Driver    string `yaml:"driver" usage:"The driver to use: one of sqlite3, mysql, or postgresql."`
	Endpoint  string `yaml:"endpoint" usage:"Typically the host:port combination of the database server."`
	Username  string `yaml:"username" usage:"Username to use when connecting."`
	Password  string `yaml:"password" usage:"Password to use when connecting. Not used if AWS IAM is enabled."`
	DBName    string `yaml:"db_name" usage:"The name of the database to use for BuildBuddy data."`
	Region    string `yaml:"region" usage:"Region of the database instance. Required if AWS IAM is enabled."`
	UseAWSIAM bool   `yaml:"use_aws_iam" usage:"If enabled, AWS IAM authentication is used instead of fixed credentials. Make sure the endpoint includes the port, otherwise IAM-based auth will fail."`
	Params    string `yaml:"params" usage:"Optional parameters to pass to the database driver (in format key1=val1&key2=val2)"`
}

// dnsFormatter generates DSN strings from structured options.
type dsnFormatter interface {
	String() string
	Driver() string
	Endpoint() string
	Username() string
	SetPassword(pw string)
	AddParam(key, val string)
	Clone() dsnFormatter
}

func newDSNFormatter(ac *AdvancedConfig) (dsnFormatter, error) {
	switch ac.Driver {
	case mysqlDriver:
		return newMysqlDSNFormatter(ac)
	case sqliteDriver:
		return newSqliteDSNFormatter(ac)
	case postgresDriver:
		return newPostgresDSNFormatter(ac)
	default:
		return nil, status.UnimplementedErrorf("newDSNFormatter does not support driver: %s", ac.Driver)
	}
}

type sqliteDSNFormatter struct {
	endpoint string
	username string
	password string
	dbName   string
	params   string
}

func newSqliteDSNFormatter(ac *AdvancedConfig) (*sqliteDSNFormatter, error) {
	return &sqliteDSNFormatter{
		endpoint: ac.Endpoint,
		username: ac.Username,
		password: ac.Password,
		dbName:   ac.DBName,
		params:   ac.Params,
	}, nil
}

func (s *sqliteDSNFormatter) AddParam(key, val string) {
	if s.params != "" {
		s.params += "&"
	}
	s.params += fmt.Sprintf("%s=%s", key, val)
}

func (s *sqliteDSNFormatter) String() string {

	b := strings.Builder{}
	if s.username != "" && s.password != "" {
		b.WriteString(s.username)
		b.WriteString(":")
		b.WriteString(s.password)
		b.WriteString("@")
	}
	b.WriteString(s.endpoint)
	if s.dbName != "" {
		b.WriteString("/")
		b.WriteString(s.dbName)
	}
	if s.params != "" {
		b.WriteString("?")
		b.WriteString(s.params)
	}
	return b.String()
}

func (_ *sqliteDSNFormatter) Driver() string {
	return sqliteDriver
}

func (s *sqliteDSNFormatter) Endpoint() string {
	return s.endpoint
}

func (s *sqliteDSNFormatter) Username() string {
	return s.username
}

func (s *sqliteDSNFormatter) SetPassword(pw string) {
	s.password = pw
}

func (s *sqliteDSNFormatter) Clone() dsnFormatter {
	c := *s
	return &c
}

type mysqlDSNFormatter struct {
	cfg *gomysql.Config
}

func newMysqlDSNFormatter(ac *AdvancedConfig) (*mysqlDSNFormatter, error) {
	cfg := gomysql.NewConfig()
	cfg.User = ac.Username
	cfg.Passwd = ac.Password
	cfg.Net = "tcp"
	cfg.Addr = ac.Endpoint
	cfg.DBName = ac.DBName
	if ac.Params != "" {
		pcfg, err := gomysql.ParseDSN("/_?" + ac.Params)
		if err != nil {
			return nil, status.FailedPreconditionErrorf("Params are invalid mysql connection params: %s", err)
		}
		cfg.Params = pcfg.Params
	} else {
		cfg.Params = make(map[string]string, 0)
	}
	return &mysqlDSNFormatter{cfg: cfg}, nil
}

func (m *mysqlDSNFormatter) AddParam(key, val string) {
	if m.cfg.Params[key] != "" {
		m.cfg.Params[key] += ","
	}
	m.cfg.Params[key] += val
}

func (m *mysqlDSNFormatter) String() string {
	return m.cfg.FormatDSN()
}

func (_ *mysqlDSNFormatter) Driver() string {
	return mysqlDriver
}

func (m *mysqlDSNFormatter) Endpoint() string {
	return m.cfg.Addr
}

func (m *mysqlDSNFormatter) Username() string {
	return m.cfg.User
}

func (m *mysqlDSNFormatter) SetPassword(pw string) {
	m.cfg.Passwd = pw
}

func (m *mysqlDSNFormatter) Clone() dsnFormatter {
	return &mysqlDSNFormatter{cfg: m.cfg.Clone()}
}

type postgresDSNFormatter struct {
	host     string
	port     string
	user     string
	password string
	dbname   string
	params   map[string][]string
}

func newPostgresDSNFormatter(ac *AdvancedConfig) (*postgresDSNFormatter, error) {
	host, port, err := net.SplitHostPort(ac.Endpoint)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("Endpoint %s for postgres is not valid: %s", ac.Endpoint, err)
	}
	params, err := url.ParseQuery(ac.Params)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("Params %s for postgres are not valid: %s", params, err)
	}
	return &postgresDSNFormatter{
		host:     host,
		port:     port,
		user:     ac.Username,
		password: ac.Password,
		dbname:   ac.DBName,
		params:   params,
	}, nil
}

func (p *postgresDSNFormatter) AddParam(key, val string) {
	p.params[key] = append(p.params[key], val)
}

func (p *postgresDSNFormatter) String() string {
	var user *url.Userinfo
	if p.password != "" {
		user = url.UserPassword(p.user, p.password)
	} else if p.user != "" {
		user = url.User(p.user)
	}
	host := p.host
	if p.port != "" {
		host = net.JoinHostPort(p.host, p.port)
	}
	dbname := p.dbname
	if !strings.HasPrefix(dbname, "/") {
		dbname = "/" + dbname
	}
	dsn := url.URL{
		Scheme:   "postgres",
		Host:     host,
		User:     user,
		Path:     dbname,
		RawQuery: url.Values(p.params).Encode(),
	}
	return dsn.String()
}

func (_ *postgresDSNFormatter) Driver() string {
	return postgresDriver
}

func (p *postgresDSNFormatter) Endpoint() string {
	return net.JoinHostPort(p.host, p.port)
}

func (p *postgresDSNFormatter) Username() string {
	return p.user
}

func (p *postgresDSNFormatter) SetPassword(pw string) {
	p.password = pw
}

func (p *postgresDSNFormatter) Clone() dsnFormatter {
	c := *p
	c.params = make(map[string][]string, len(p.params))
	for k, v := range p.params {
		copy(c.params[k], v)
	}
	return &c
}

type DBHandle struct {
	db            *gorm.DB
	readReplicaDB *gorm.DB
	driver        string
}

type Options struct {
	readOnly        bool
	allowStaleReads bool
}

func Opts() interfaces.DBOptions {
	return &Options{}
}

func (o *Options) WithStaleReads() interfaces.DBOptions {
	o.readOnly = true
	o.allowStaleReads = true
	return o
}

func (o *Options) ReadOnly() bool {
	return o.readOnly
}

func (o *Options) AllowStaleReads() bool {
	return o.allowStaleReads
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
	return db
}

func IsRecordNotFound(err error) bool {
	return errors.Is(err, gorm.ErrRecordNotFound)
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

func recordMetricsBeforeFn(db *gorm.DB) {
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
		metrics.SQLQueryDurationUsec.With(labels).Observe(float64(time.Since(opStartTime).Microseconds()))
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
	gormutil.InstrumentMetrics(gdb, gormRecordOpStartTimeCallbackKey, recordMetricsBeforeFn, gormRecordMetricsCallbackKey, recordMetricsAfterFn)

	gdb.Callback().Create().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:create"))
	gdb.Callback().Delete().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:delete"))
	gdb.Callback().Query().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:query"))
	gdb.Callback().Raw().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:raw"))
	gdb.Callback().Row().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:row"))
	gdb.Callback().Update().Before("*").Register(gormStartSpanCallbackKey, makeStartSpanBeforeFn("gorm:update"))

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

func openDB(ctx context.Context, dataSource string, advancedConfig *AdvancedConfig) (*gorm.DB, string, error) {
	ds, err := ParseDatasource(ctx, dataSource, advancedConfig)
	if err != nil {
		return nil, "", err
	}

	drv, err := getDriver(ds)
	if err != nil {
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
	case postgresDriver:
		dialector = postgres.Dialector{Config: &postgres.Config{Conn: db}}
	default:
		return nil, "", fmt.Errorf("unsupported database driver %s", ds.DriverName())
	}

	l := &sqlLogger{
		SlowThreshold: *slowQueryThreshold,
		LogLevel:      logger.Warn,
	}
	if *logQueries {
		l.LogLevel = logger.Info
	}
	config := gorm.Config{Logger: l, SkipDefaultTransaction: true}
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
	baseDSN dsnFormatter
	region  string
	cfg     *aws.Config
}

func (aid *awsIAMDataSource) DriverName() string {
	return aid.baseDSN.Driver()
}

func (aid *awsIAMDataSource) DSN() (string, error) {
	creds := aid.cfg.Credentials
	token, err := rdsauth.BuildAuthToken(context.Background(), aid.baseDSN.Endpoint(), aid.region, aid.baseDSN.Username(), creds)
	if err != nil {
		return "", status.UnavailableErrorf("could not obtain AWS IAM auth token: %s", err)
	}
	dsn := aid.baseDSN.Clone()
	dsn.SetPassword(token)
	return dsn.String(), nil
}

func loadAWSRDSCACerts(fileResolver fs.FS) (*x509.CertPool, error) {
	certPool := x509.NewCertPool()
	f, err := fileResolver.Open("aws_rds_certs/rds-combined-ca-bundle.pem")
	if err != nil {
		return nil, status.UnavailableErrorf("could not open RDS CA bundle: %s", err)
	}
	defer f.Close()
	pem, err := io.ReadAll(f)
	if err != nil {
		return nil, status.UnavailableErrorf("could not read RDS CA bundle: %s", err)
	}
	if !certPool.AppendCertsFromPEM(pem) {
		return nil, status.UnavailableErrorf("could not parse RDS CA bundle")
	}
	return certPool, nil
}

func ParseDatasource(ctx context.Context, datasource string, advancedConfig *AdvancedConfig) (DataSource, error) {
	if *advancedConfig != (AdvancedConfig{}) {
		ac := advancedConfig
		dsn, err := newDSNFormatter(ac)
		if err != nil {
			return nil, err
		}

		if ac.Endpoint == "" {
			return nil, status.FailedPreconditionError("endpoint is required")
		}

		if ac.Driver == mysqlDriver {
			dsn.AddParam("sql_mode", "ANSI_QUOTES")
		}

		if ac.UseAWSIAM {
			if ac.Region == "" {
				return nil, status.FailedPreconditionError("region is required to enable AWS IAM")
			}
			if ac.Password != "" {
				return nil, status.FailedPreconditionError("password should not be specified when AWS IAM is enabled")
			}

			if ac.Driver == mysqlDriver {
				certPool, err := loadAWSRDSCACerts(aws_rds_certs.Get())
				if err != nil {
					return nil, err
				}
				if err = gomysql.RegisterTLSConfig("rds", &tls.Config{RootCAs: certPool}); err != nil {
					return nil, status.UnknownErrorf("could not configure RDS CA bundle for mysql: %s", err)
				}
				dsn.AddParam("tls", "rds")
				dsn.AddParam("allowCleartextPasswords", "true")
			}

			cfg, err := awsconfig.LoadDefaultConfig(ctx)
			if err != nil {
				return nil, status.FailedPreconditionErrorf("could not initialize AWS session: %s", err)
			}
			return &awsIAMDataSource{
				baseDSN: dsn,
				region:  ac.Region,
				cfg:     &cfg,
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
		switch driverName {
		case "postgres":
			driverName = postgresDriver
			fallthrough
		case postgresDriver:
			connString = datasource
		case mysqlDriver:
			cfg, err := gomysql.ParseDSN(connString)
			if err != nil {
				return nil, fmt.Errorf("malformed mySQL connection string: %s", err)
			}
			if cfg.Params == nil {
				cfg.Params = make(map[string]string, 1)
			}
			if cfg.Params["sql_mode"] != "" {
				cfg.Params["sql_mode"] += ","
			}
			cfg.Params["sql_mode"] += "ANSI_QUOTES"
			connString = cfg.FormatDSN()
		}
		return &fixedDSNDataSource{driver: driverName, dsn: connString}, nil
	}

	return nil, status.FailedPreconditionError("no database configured -- please specify at least one in the config")
}

// sqlLogger implements GORM's logger.Interface using zerolog.
//
// TODO: maybe implement the optional ParamsFilter interface so that we only log
// parameterized queries.
type sqlLogger struct {
	SlowThreshold time.Duration
	LogLevel      logger.LogLevel
}

func (l *sqlLogger) Info(ctx context.Context, format string, args ...any) {
	log.CtxInfof(ctx, "%s: "+format, append([]any{gormutils.FileWithLineNum()}, args...))
}
func (l *sqlLogger) Warn(ctx context.Context, format string, args ...any) {
	log.CtxWarningf(ctx, "%s: "+format, append([]any{gormutils.FileWithLineNum()}, args...))
}
func (l *sqlLogger) Error(ctx context.Context, format string, args ...any) {
	log.CtxErrorf(ctx, "%s: "+format, append([]any{gormutils.FileWithLineNum()}, args...))
}

// Trace is called after every SQL query. If `database.log_queries` is true then
// it will always log the query. Otherwise it will only log slow or failed
// queries. NotFound errors are ignored.
func (l *sqlLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	if l.LogLevel <= logger.Silent {
		return
	}
	duration := time.Since(begin)
	getInfo := func() string {
		sql, rows := fc()
		rowsVal := any(rows)
		if rows <= 0 {
			// rows < 0 means the query does not have an associated row count.
			rowsVal = "-"
		}
		return fmt.Sprintf("(duration: %s) (rows: %v) %s", duration, rowsVal, sql)
	}
	switch {
	case err != nil && l.LogLevel >= logger.Error && !IsRecordNotFound(err):
		log.CtxErrorf(ctx, "SQL: error (%s): %s %s", gormutils.FileWithLineNum(), err, getInfo())
	case duration > l.SlowThreshold && l.SlowThreshold != 0 && l.LogLevel >= logger.Warn:
		log.CtxWarningf(ctx, "SQL: slow query (over %s) (%s): %s", l.SlowThreshold, gormutils.FileWithLineNum(), getInfo())
	case l.LogLevel == logger.Info:
		log.CtxInfof(ctx, "SQL: OK (%s) %s", gormutils.FileWithLineNum(), getInfo())
	}
}

func (l *sqlLogger) LogMode(level logger.LogLevel) logger.Interface {
	clone := *l
	clone.LogLevel = level
	return &clone
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

func GetConfiguredDatabase(ctx context.Context, env environment.Env) (interfaces.DBHandle, error) {
	if *dataSource == "" {
		return nil, fmt.Errorf("No database configured -- please specify one in the config")
	}

	tables.RegisterTables()

	// Verify that the AWS RDS certs are properly packaged.
	// They won't actually be used unless the AWS IAM feature is enabled.
	_, err := loadAWSRDSCACerts(aws_rds_certs.Get())
	if err != nil {
		return nil, err
	}

	primaryDB, driverName, err := openDB(ctx, *dataSource, advDataSource)
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

	if *autoMigrateDB || *autoMigrateDBAndExit || *printSchemaChangesAndExit {
		sqlStrings := make([]string, 0)
		if *printSchemaChangesAndExit {
			primaryDB.Logger = logger.Default.LogMode(logger.Silent)
			if err := gormutil.RegisterLogSQLCallback(primaryDB, &sqlStrings); err != nil {
				return nil, err
			}
		}

		if err := runMigrations(driverName, primaryDB); err != nil {
			return nil, err
		}

		if *printSchemaChangesAndExit {
			gormutil.PrintMigrationSchemaChanges(sqlStrings)
		}

		if *autoMigrateDBAndExit || *printSchemaChangesAndExit {
			log.Infof("Database migration completed. Exiting due to --auto_migrate_db_and_exit or --database.print_schema_changes_and_exit.")
			os.Exit(0)
		}
	}

	dbh := &DBHandle{
		db:     primaryDB,
		driver: driverName,
	}
	env.GetHealthChecker().AddHealthCheck("sql_primary", interfaces.CheckerFunc(func(ctx context.Context) error {
		return primarySQLDB.Ping()
	}))

	// Setup a read replica if one is configured.
	if *readReplica != "" {
		replicaDB, readDialect, err := openDB(ctx, *readReplica, advReadReplica)
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

		env.GetHealthChecker().AddHealthCheck("sql_read_replica", interfaces.CheckerFunc(func(ctx context.Context) error {
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
	switch h.driver {
	case sqliteDriver:
		return `STRFTIME('%Y-%m', ` + timestampExpr + `, 'unixepoch')`
	case mysqlDriver:
		return `DATE_FORMAT(FROM_UNIXTIME(` + timestampExpr + `), '%Y-%m')`
	case postgresDriver:
		return `TO_CHAR(TO_TIMESTAMP(` + timestampExpr + `), 'YYYY-MM')`
	default:
		log.Errorf("Driver %s is not supported by UTCMonthFromUsecTimestamp.", h.driver)
		return `UNIMPLEMENTED`
	}
}

// DateFromUsecTimestamp returns an SQL expression that converts the value
// of the given field from a Unix timestamp (in microseconds since the Unix
// Epoch) to a date offset by the given UTC offset. The offset is defined
// according to the description here:
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/getTimezoneOffset
func (h *DBHandle) DateFromUsecTimestamp(fieldName string, timezoneOffsetMinutes int32) string {
	offsetUsec := int64(timezoneOffsetMinutes) * 60 * 1e6
	timestampExpr := fmt.Sprintf("(%s + (%d))/1000000", fieldName, -offsetUsec)
	switch h.driver {
	case sqliteDriver:
		return fmt.Sprintf("DATE(%s, 'unixepoch')", timestampExpr)
	case mysqlDriver:
		return fmt.Sprintf("DATE(FROM_UNIXTIME(%s))", timestampExpr)
	case postgresDriver:
		return `TO_TIMESTAMP(` + timestampExpr + `)::DATE`
	default:
		log.Errorf("Driver %s is not supported by DateFromUsecTimestamp.", h.driver)
		return `UNIMPLEMENTED`
	}
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

func (h *DBHandle) NowFunc() time.Time {
	return h.db.NowFunc()
}

func (h *DBHandle) SetNowFunc(now func() time.Time) {
	h.db.Config.NowFunc = now
}

func (h *DBHandle) IsDeadlockError(err error) bool {
	var mysqlErr *gomysql.MySQLError
	// Defined at https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html#error_er_lock_deadlock
	if errors.As(err, &mysqlErr) && mysqlErr.Number == 1213 {
		return true
	}
	var postgresqlErr *gopostgresconn.PgError
	if errors.As(err, &postgresqlErr) && postgresqlErr.SQLState() == gopostgreserr.DeadlockDetected {
		return true
	}
	return false
}

func (dbh *DBHandle) GORM(ctx context.Context, name string) *gorm.DB {
	return dbh.db.WithContext(ctx).Set(gormQueryNameKey, name)
}

type rawQuery struct {
	db     *gorm.DB
	ctx    context.Context
	sql    string
	values []interface{}
}

func (r *rawQuery) Exec() interfaces.DBResult {
	rb := r.db.Exec(r.sql, r.values...)
	return interfaces.DBResult{Error: rb.Error, RowsAffected: rb.RowsAffected}
}

func (r *rawQuery) Take(dest interface{}) error {
	return r.db.Raw(r.sql, r.values...).Take(dest).Error
}

func (r *rawQuery) Scan(dest interface{}) error {
	return r.db.Raw(r.sql, r.values...).Scan(dest).Error
}

func (r *rawQuery) DB() *gorm.DB {
	return r.db
}

// TODO(vadim): check if there are any uses cases where ScanEach can't be used directly
func (r *rawQuery) IterateRaw(fn func(ctx context.Context, row *sql.Rows) error) error {
	rows, err := r.db.Raw(r.sql, r.values...).Rows()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := fn(r.ctx, rows); err != nil {
			return err
		}
	}
	if err != nil {
		return err
	}
	return nil
}

type query struct {
	db   *gorm.DB
	ctx  context.Context
	name string
}

func (q *query) Create(val interface{}) error {
	db := q.db.WithContext(q.ctx).Set(gormQueryNameKey, q.name)
	return db.Create(val).Error
}

func (q *query) Update(val interface{}) error {
	db := q.db.WithContext(q.ctx).Set(gormQueryNameKey, q.name)
	res := db.Updates(val)
	if res.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func (q *query) Raw(sql string, values ...interface{}) interfaces.DBRawQuery {
	db := q.db.WithContext(q.ctx).Set(gormQueryNameKey, q.name)
	return &rawQuery{db: db, ctx: q.ctx, sql: sql, values: values}
}

type transaction struct {
	tx  *gorm.DB
	ctx context.Context
}

func (t *transaction) NewQuery(ctx context.Context, name string) interfaces.DBQuery {
	return &query{ctx: ctx, name: name, db: t.tx}
}

func (t *transaction) GORM(ctx context.Context, name string) *gorm.DB {
	return t.tx.Set(gormQueryNameKey, name)
}

func (t *transaction) NowFunc() time.Time {
	return t.tx.NowFunc()
}

func (dbh *DBHandle) NewQuery(ctx context.Context, name string) interfaces.DBQuery {
	return &query{ctx: ctx, name: name, db: dbh.db}
}

func (dbh *DBHandle) NewQueryWithOpts(ctx context.Context, name string, opts interfaces.DBOptions) interfaces.DBQuery {
	return &query{ctx: ctx, name: name, db: dbh.gormHandleForOpts(ctx, opts)}
}

func (dbh *DBHandle) Transaction(ctx context.Context, txn interfaces.NewTxRunner) error {
	return dbh.DB(ctx).Transaction(func(tx *gorm.DB) error {
		return txn(&transaction{tx, ctx})
	})
}

func (dbh *DBHandle) TransactionWithOptions(ctx context.Context, opts interfaces.DBOptions, txn interfaces.NewTxRunner) error {
	return dbh.gormHandleForOpts(ctx, opts).Transaction(func(tx *gorm.DB) error {
		return txn(&transaction{tx, ctx})
	})
}

// TableSchema can be used to get the schema for a given table.
func TableSchema(db *DB, model any) (*schema.Schema, error) {
	stmt := &gorm.Statement{DB: db}
	// Note: Parse() is cheap since gorm uses a cache internally.
	if err := stmt.Parse(model); err != nil {
		return nil, err
	}
	return stmt.Schema, nil
}

type gormGetter interface {
	DB() *gorm.DB
}

// ScanEach executes the given query and iterates over the result,
// automatically scanning each row into a struct of the specified type.
//
// Example:
//
//	err := db.ScanEach(rq, func(ctx context.Context, foo *tables.Foo) error {
//	  // handle foo
//	  return nil
//	})
func ScanEach[T any](rq interfaces.DBRawQuery, fn func(ctx context.Context, val *T) error) error {
	return rq.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
		var val T
		if err := rq.(gormGetter).DB().ScanRows(row, &val); err != nil {
			return err
		}
		if err := fn(ctx, &val); err != nil {
			return err
		}
		return nil
	})
}

// ScanAll executes the given query and scans all the resulting rows into a
// slice of the specified type.
//
// This should not be used if you expect the result set size to be large.
//
// Example:
//
//	foos, err := db.ScanAll(rq, &tables.Foo{})
func ScanAll[T any](rq interfaces.DBRawQuery, t *T) ([]*T, error) {
	var vals []*T
	err := rq.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
		var val T
		if err := rq.(gormGetter).DB().ScanRows(row, &val); err != nil {
			return err
		}
		vals = append(vals, &val)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return vals, nil
}

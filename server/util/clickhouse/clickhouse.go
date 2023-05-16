package clickhouse

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/gormutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	gormclickhouse "gorm.io/driver/clickhouse"
)

const (
	gormStmtStartTimeKey             = "bb_clickhouse:op_start_time"
	gormRecordOpStartTimeCallbackKey = "bb_clickhouse:record_op_start_time"
	gormRecordMetricsCallbackKey     = "bb_clickhouse:record_metrics"
	gormQueryNameKey                 = "bb_clickhouse:query_name"
)

var (
	dataSource      = flagutil.New("olap_database.data_source", "", "The clickhouse database to connect to, specified a a connection string", flagutil.SecretTag)
	maxOpenConns    = flag.Int("olap_database.max_open_conns", 0, "The maximum number of open connections to maintain to the db")
	maxIdleConns    = flag.Int("olap_database.max_idle_conns", 0, "The maximum number of idle connections to maintain to the db")
	connMaxLifetime = flag.Duration("olap_database.conn_max_lifetime", 0, "The maximum lifetime of a connection to clickhouse")

	autoMigrateDB             = flag.Bool("olap_database.auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
	printSchemaChangesAndExit = flag.Bool("olap_database.print_schema_changes_and_exit", false, "If set, print schema changes from auto-migration, then exit the program.")
)

type DBHandle struct {
	db *gorm.DB
}

type Options struct {
	queryName string
}

func Opts() interfaces.OLAPDBOptions {
	return &Options{}
}

// WithQueryName specifies the query label to use in exported metrics.
func (o *Options) WithQueryName(queryName string) interfaces.OLAPDBOptions {
	o.queryName = queryName
	return o
}

func (o *Options) QueryName() string {
	return o.queryName
}

func (dbh *DBHandle) gormHandleForOpts(ctx context.Context, opts interfaces.OLAPDBOptions) *gorm.DB {
	db := dbh.DB(ctx)
	if opts.QueryName() != "" {
		db = db.Set(gormQueryNameKey, opts.QueryName())
	}
	return db
}

func (dbh *DBHandle) RawWithOptions(ctx context.Context, opts interfaces.OLAPDBOptions, sql string, values ...interface{}) *gorm.DB {
	return dbh.gormHandleForOpts(ctx, opts).Raw(sql, values...)
}

func (dbh *DBHandle) DB(ctx context.Context) *gorm.DB {
	return dbh.db.WithContext(ctx)
}

// DateFromUsecTimestamp returns an SQL expression compatible with clickhouse
// that converts the value of the given field from a Unix timestamp (in
// microseconds since the Unix Epoch) to a date offset by the given UTC offset.
// The offset is defined according to the description here:
// https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date/getTimezoneOffset
func (h *DBHandle) DateFromUsecTimestamp(fieldName string, timezoneOffsetMinutes int32) string {
	offsetUsec := int64(timezoneOffsetMinutes) * 60 * 1e6
	timestampExpr := fmt.Sprintf("intDiv(%s + (%d), 1000000)", fieldName, -offsetUsec)
	return fmt.Sprintf("FROM_UNIXTIME(%s,", timestampExpr) + "'%F')"
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "i/o timeout")
}

func (h *DBHandle) insertWithRetrier(ctx context.Context, tableName string, numEntries int, value interface{}) error {
	retrier := retry.DefaultWithContext(ctx)
	var lastError error
	for retrier.Next() {
		res := h.DB(ctx).Create(value)
		lastError = res.Error
		if errors.Is(res.Error, syscall.ECONNRESET) || errors.Is(res.Error, syscall.ECONNREFUSED) || isTimeout(res.Error) {
			// Retry since it's an transient error.
			log.CtxWarningf(ctx, "attempt (n=%d) to clickhouse table %q failed: %s", retrier.AttemptNumber(), tableName, res.Error)
			continue
		}
		break
	}
	statusLabel := "ok"
	if lastError != nil {
		statusLabel = "error"
	}
	metrics.ClickhouseInsertedCount.With(prometheus.Labels{
		metrics.ClickhouseTableName:   tableName,
		metrics.ClickhouseStatusLabel: statusLabel,
	}).Add(float64(numEntries))
	if lastError != nil {
		return status.UnavailableErrorf("insertWithRetrier exceeded retries (n=%d), err: %s", retrier.AttemptNumber(), lastError)
	}
	return lastError

}

func (h *DBHandle) FlushInvocationStats(ctx context.Context, ti *tables.Invocation) error {
	inv := schema.ToInvocationFromPrimaryDB(ti)
	if err := h.insertWithRetrier(ctx, inv.TableName(), 1, inv); err != nil {
		return status.UnavailableErrorf("failed to insert invocation (invocation_id = %q), err: %s", ti.InvocationID, err)
	}
	return nil
}

func buildExecution(in *repb.StoredExecution, inv *sipb.StoredInvocation) *schema.Execution {
	return &schema.Execution{
		GroupID:                            in.GetGroupId(),
		UpdatedAtUsec:                      in.GetUpdatedAtUsec(),
		ExecutionID:                        in.GetExecutionId(),
		InvocationUUID:                     in.GetInvocationUuid(),
		CreatedAtUsec:                      in.GetCreatedAtUsec(),
		UserID:                             in.GetUserId(),
		Worker:                             in.GetWorker(),
		Stage:                              in.GetStage(),
		FileDownloadCount:                  in.GetFileDownloadCount(),
		FileDownloadSizeBytes:              in.GetFileDownloadSizeBytes(),
		FileDownloadDurationUsec:           in.GetFileDownloadDurationUsec(),
		FileUploadCount:                    in.GetFileUploadCount(),
		FileUploadSizeBytes:                in.GetFileUploadSizeBytes(),
		FileUploadDurationUsec:             in.GetFileUploadDurationUsec(),
		PeakMemoryBytes:                    in.GetPeakMemoryBytes(),
		CPUNanos:                           in.GetCpuNanos(),
		EstimatedMemoryBytes:               in.GetEstimatedMemoryBytes(),
		EstimatedMilliCPU:                  in.GetEstimatedMilliCpu(),
		QueuedTimestampUsec:                in.GetQueuedTimestampUsec(),
		WorkerStartTimestampUsec:           in.GetWorkerStartTimestampUsec(),
		WorkerCompletedTimestampUsec:       in.GetWorkerCompletedTimestampUsec(),
		InputFetchStartTimestampUsec:       in.GetInputFetchStartTimestampUsec(),
		InputFetchCompletedTimestampUsec:   in.GetInputFetchCompletedTimestampUsec(),
		ExecutionStartTimestampUsec:        in.GetExecutionStartTimestampUsec(),
		ExecutionCompletedTimestampUsec:    in.GetExecutionCompletedTimestampUsec(),
		OutputUploadStartTimestampUsec:     in.GetOutputUploadStartTimestampUsec(),
		OutputUploadCompletedTimestampUsec: in.GetOutputUploadCompletedTimestampUsec(),
		InvocationLinkType:                 int8(in.GetInvocationLinkType()),
		User:                               inv.GetUser(),
		Host:                               inv.GetHost(),
		Pattern:                            inv.GetPattern(),
		Role:                               inv.GetRole(),
		BranchName:                         inv.GetBranchName(),
		CommitSHA:                          inv.GetCommitSha(),
		RepoURL:                            inv.GetRepoUrl(),
		Command:                            inv.GetCommand(),
		Success:                            inv.GetSuccess(),
		InvocationStatus:                   inv.GetInvocationStatus(),
		Tags:                               invocation_format.ConvertDbTagsToOlap(inv.GetTags()),
	}
}

func (h *DBHandle) FlushExecutionStats(ctx context.Context, inv *sipb.StoredInvocation, executions []*repb.StoredExecution) error {
	entries := make([]*schema.Execution, 0, len(executions))
	for _, e := range executions {
		entries = append(entries, buildExecution(e, inv))
	}
	num := len(entries)
	if err := h.insertWithRetrier(ctx, (&schema.Execution{}).TableName(), num, &entries); err != nil {
		return status.UnavailableErrorf("failed to insert %d execution(s) for invocation (invocation_id = %q), err: %s", num, inv.GetInvocationId(), err)
	}
	return nil
}

func (h *DBHandle) FlushTestTargetStatuses(ctx context.Context, entries []*schema.TestTargetStatus) error {
	num := len(entries)
	if num == 0 {
		return nil
	}
	if err := h.insertWithRetrier(ctx, (&schema.TestTargetStatus{}).TableName(), num, &entries); err != nil {
		return status.UnavailableErrorf("failed to insert %d test target statuses for invocation (invocation_uuid = %q), err: %s", num, entries[0].InvocationUUID, err)
	}
	return nil
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

	metrics.ClickhouseQueryCount.With(labels).Inc()

	// v will be nil if our key is not in the map so we can ignore the presence indicator.
	v, _ := db.Statement.Settings.LoadAndDelete(gormStmtStartTimeKey)
	if opStartTime, ok := v.(time.Time); ok {
		metrics.ClickhouseQueryDurationUsec.With(labels).Observe(float64(time.Now().Sub(opStartTime).Microseconds()))
	}
	// Ignore "record not found" errors as they don't generally indicate a
	// problem with the server.
	if db.Error != nil && !errors.Is(db.Error, gorm.ErrRecordNotFound) {
		metrics.ClickhouseQueryErrorCount.With(labels).Inc()
	}
}

func recordMetricsBeforeFn(db *gorm.DB) {
	if db.DryRun || db.Statement == nil {
		return
	}
	db.Statement.Settings.Store(gormStmtStartTimeKey, time.Now())
}

func Register(env environment.Env) error {
	if *dataSource == "" {
		return nil
	}
	options, err := clickhouse.ParseDSN(*dataSource)
	if err != nil {
		return status.InternalErrorf("failed to parse clickhouse data source (%q): %s", *dataSource, err)
	}

	sqlDB := clickhouse.OpenDB(options)
	if *maxOpenConns != 0 {
		sqlDB.SetMaxOpenConns(*maxOpenConns)
	}
	if *maxIdleConns != 0 {
		sqlDB.SetMaxIdleConns(*maxIdleConns)
	}
	if *connMaxLifetime != 0 {
		sqlDB.SetConnMaxLifetime(*connMaxLifetime)
	}

	db, err := gorm.Open(gormclickhouse.New(gormclickhouse.Config{
		Conn:                         sqlDB,
		DontSupportEmptyDefaultValue: true,
	}))
	if err != nil {
		return status.InternalErrorf("failed to open gorm clickhouse db: %s", err)
	}
	gormutil.InstrumentMetrics(db, gormRecordOpStartTimeCallbackKey, recordMetricsBeforeFn, gormRecordMetricsCallbackKey, recordMetricsAfterFn)
	if *autoMigrateDB || *printSchemaChangesAndExit {
		sqlStrings := make([]string, 0)
		if *printSchemaChangesAndExit {
			db.Logger = logger.Default.LogMode(logger.Silent)
			if err := gormutil.RegisterLogSQLCallback(db, &sqlStrings); err != nil {
				return err
			}
		}

		if err := schema.RunMigrations(db); err != nil {
			return err
		}

		if *printSchemaChangesAndExit {
			gormutil.PrintMigrationSchemaChanges(sqlStrings)
			log.Info("Clickhouse migration completed. Exiting due to --clickhouse.print_schema_changes_and_exit.")
			os.Exit(0)
		}
	}

	dbh := &DBHandle{
		db: db,
	}

	env.SetOLAPDBHandle(dbh)
	return nil
}

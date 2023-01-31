package clickhouse

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"strings"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	gormclickhouse "gorm.io/driver/clickhouse"
)

var (
	dataSource      = flagutil.New("olap_database.data_source", "", "The clickhouse database to connect to, specified a a connection string", flagutil.SecretTag)
	maxOpenConns    = flag.Int("olap_database.max_open_conns", 0, "The maximum number of open connections to maintain to the db")
	maxIdleConns    = flag.Int("olap_database.max_idle_conns", 0, "The maximum number of idle connections to maintain to the db")
	connMaxLifetime = flag.Duration("olap_database.conn_max_lifetime", 0, "The maximum lifetime of a connection to clickhouse")

	autoMigrateDB = flag.Bool("olap_database.auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
)

type DBHandle struct {
	db *gorm.DB
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
		Conn: sqlDB,
	}))
	if err != nil {
		return status.InternalErrorf("failed to open gorm clickhouse db: %s", err)
	}
	if *autoMigrateDB {
		if err := schema.RunMigrations(db); err != nil {
			return err
		}
	}

	dbh := &DBHandle{
		db: db,
	}

	env.SetOLAPDBHandle(dbh)
	return nil
}

package clickhouse

import (
	"context"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"strings"
	"syscall"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gormclickhouse "gorm.io/driver/clickhouse"
)

var (
	dataSource      = flagutil.New("olap_database.data_source", "", "The clickhouse database to connect to, specified a a connection string", flagutil.SecretTag)
	maxOpenConns    = flag.Int("olap_database.max_open_conns", 0, "The maximum number of open connections to maintain to the db")
	maxIdleConns    = flag.Int("olap_database.max_idle_conns", 0, "The maximum number of idle connections to maintain to the db")
	connMaxLifetime = flag.Duration("olap_database.conn_max_lifetime", 0, "The maximum lifetime of a connection to clickhouse")

	autoMigrateDB = flag.Bool("olap_database.auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")

	// {installation}, {cluster}, {shard}, {replica} are macros provided by
	// Altinity/clickhouse-operator; {database}, {table} are macros provided by clickhouse.
	dataReplicationEnabled = flag.Bool("olap_database.enable_data_replication", false, "If true, data replication is enabled.")
	zooPath                = flag.String("olap_database.zoo_path", "/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}", "The path to the table name in zookeeper, used to set up data replication")
	replicaName            = flag.String("olap_database.replica_name", "{replica}", "The replica name of the table in zookeeper")
	clusterName            = flag.String("olap_database.cluster_name", "{cluster}", "The cluster name of the database")
)

type DBHandle struct {
	db *gorm.DB
}

func (dbh *DBHandle) DB(ctx context.Context) *gorm.DB {
	return dbh.db.WithContext(ctx)
}

// Making a new table? Please make sure you:
// 1) Add your table in getAllTables()
// 2) Add the table in clickhouse_test.go TestSchemaInSync
// 3) Make sure all the fields in the corresponding Table deinition in tables.go
// are present in clickhouse Table definition or in ExcludedFields()
type Table interface {
	TableName() string
	TableOptions() string
	// Fields that are in the primary DB Table schema; but not in the clickhouse schema.
	ExcludedFields() []string
	// Fields that are in the clickhouse Table schema; but not in the primary DB Table Schema.
	AdditionalFields() []string
}

func GetAllTables() []Table {
	return []Table{
		&Invocation{},
		&Execution{},
	}
}

func getTableClusterOption() string {
	if *dataReplicationEnabled {
		return fmt.Sprintf("on cluster '%s'", *clusterName)
	}
	return ""
}

func getEngine() string {
	if *dataReplicationEnabled {
		return fmt.Sprintf("ReplicatedReplacingMergeTree('%s', '%s')", *zooPath, *replicaName)
	}
	return "ReplacingMergeTree()"
}

func getAllTables() []Table {
	return []Table{
		&Invocation{},
	}
}

func tableClusterOption() string {
	if *dataReplicationEnabled {
		return fmt.Sprintf("on cluster '%s'", *clusterName)
	}
	return ""
}

// Invocation constains a subset of tables.Invocations.
type Invocation struct {
	GroupID        string `gorm:"primaryKey;"`
	UpdatedAtUsec  int64  `gorm:"primaryKey;"`
	CreatedAtUsec  int64
	InvocationUUID string
	Role           string
	User           string
	Host           string
	CommitSHA      string
	BranchName     string
	Command        string
	BazelExitCode  string

	UserID           string
	Pattern          string
	InvocationStatus int64
	Attempt          uint64

	ActionCount                       int64
	RepoURL                           string
	DurationUsec                      int64
	Success                           bool
	ActionCacheHits                   int64
	ActionCacheMisses                 int64
	ActionCacheUploads                int64
	CasCacheHits                      int64
	CasCacheMisses                    int64
	CasCacheUploads                   int64
	TotalDownloadSizeBytes            int64
	TotalUploadSizeBytes              int64
	TotalDownloadTransferredSizeBytes int64
	TotalUploadTransferredSizeBytes   int64
	TotalDownloadUsec                 int64
	TotalUploadUsec                   int64
	TotalCachedActionExecUsec         int64
	DownloadThroughputBytesPerSecond  int64
	UploadThroughputBytesPerSecond    int64
}

func (i *Invocation) ExcludedFields() []string {
	return []string{
		"InvocationID",
		"BlobID",
		"LastChunkId",
		"RedactionFlags",
		"CreatedWithCapabilities",
		"Perms",
	}
}

func (i *Invocation) AdditionalFields() []string {
	return []string{}
}

func (i *Invocation) TableName() string {
	return "Invocations"
}

func (i *Invocation) TableOptions() string {
	// Note: the sorting key need to be able to uniquely identify the invocation.
	// ReplacingMergeTree will remove entries with the same sorting key in the background.
	return fmt.Sprintf("ENGINE=%s ORDER BY (group_id, updated_at_usec, invocation_uuid)", getEngine())
}

type Execution struct {
	// Sort keys
	GroupID        string
	UpdatedAtUsec  int64
	InvocationUUID string
	ExecutionID    string

	// Type from tables.InvocationExecution
	InvocationLinkType int8
	CreatedAtUsec      int64
	UserID             string
	Worker             string

	Stage int64

	// IOStats
	FileDownloadCount        int64
	FileDownloadSizeBytes    int64
	FileDownloadDurationUsec int64
	FileUploadCount          int64
	FileUploadSizeBytes      int64
	FileUploadDurationUsec   int64

	// UsageStats
	PeakMemoryBytes int64
	CPUNanos        int64

	// Task sizing
	EstimatedMemoryBytes int64
	EstimatedMilliCPU    int64

	// ExecutedActionMetadata (in addition to Worker above)
	QueuedTimestampUsec                int64
	WorkerStartTimestampUsec           int64
	WorkerCompletedTimestampUsec       int64
	InputFetchStartTimestampUsec       int64
	InputFetchCompletedTimestampUsec   int64
	ExecutionStartTimestampUsec        int64
	ExecutionCompletedTimestampUsec    int64
	OutputUploadStartTimestampUsec     int64
	OutputUploadCompletedTimestampUsec int64

	StatusCode int32
	ExitCode   int32

	CachedResult bool
	DoNotCache   bool

	// Fields from Invocations
	User             string
	Host             string
	Pattern          string
	Role             string
	BranchName       string
	CommitSHA        string
	RepoURL          string
	Command          string
	InvocationStatus int64
	Success          bool
}

func (e *Execution) TableName() string {
	return "Executions"
}

func (e *Execution) TableOptions() string {
	return fmt.Sprintf("ENGINE=%s ORDER BY (group_id, updated_at_usec, invocation_uuid,execution_id)", getEngine())
}

func (e *Execution) ExcludedFields() []string {
	return []string{
		"InvocationID",
		"Perms",
		"SerializedOperation",
		"SerializedStatusDetails",
		"CommandSnippet",
		"StatusMessage",
	}
}

func (e *Execution) AdditionalFields() []string {
	return []string{
		"InvocationUUID",
		"User",
		"Host",
		"Pattern",
		"Role",
		"BranchName",
		"CommitSHA",
		"RepoURL",
		"Command",
		"InvocationStatus",
		"Success",
		"InvocationLinkType",
	}
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

func ToInvocationFromPrimaryDB(ti *tables.Invocation) *Invocation {
	return &Invocation{
		GroupID:                           ti.GroupID,
		UpdatedAtUsec:                     ti.UpdatedAtUsec,
		CreatedAtUsec:                     ti.CreatedAtUsec,
		InvocationUUID:                    hex.EncodeToString(ti.InvocationUUID),
		Role:                              ti.Role,
		User:                              ti.User,
		UserID:                            ti.UserID,
		Host:                              ti.Host,
		CommitSHA:                         ti.CommitSHA,
		BranchName:                        ti.BranchName,
		Command:                           ti.Command,
		BazelExitCode:                     ti.BazelExitCode,
		Pattern:                           ti.Pattern,
		Attempt:                           ti.Attempt,
		ActionCount:                       ti.ActionCount,
		InvocationStatus:                  ti.InvocationStatus,
		RepoURL:                           ti.RepoURL,
		DurationUsec:                      ti.DurationUsec,
		Success:                           ti.Success,
		ActionCacheHits:                   ti.ActionCacheHits,
		ActionCacheMisses:                 ti.ActionCacheMisses,
		ActionCacheUploads:                ti.ActionCacheUploads,
		CasCacheHits:                      ti.CasCacheHits,
		CasCacheMisses:                    ti.CasCacheMisses,
		CasCacheUploads:                   ti.CasCacheUploads,
		TotalDownloadSizeBytes:            ti.TotalDownloadSizeBytes,
		TotalUploadSizeBytes:              ti.TotalUploadSizeBytes,
		TotalDownloadUsec:                 ti.TotalDownloadUsec,
		TotalUploadUsec:                   ti.TotalUploadUsec,
		TotalDownloadTransferredSizeBytes: ti.TotalDownloadTransferredSizeBytes,
		TotalUploadTransferredSizeBytes:   ti.TotalUploadTransferredSizeBytes,
		TotalCachedActionExecUsec:         ti.TotalCachedActionExecUsec,
		DownloadThroughputBytesPerSecond:  ti.DownloadThroughputBytesPerSecond,
		UploadThroughputBytesPerSecond:    ti.UploadThroughputBytesPerSecond,
	}
}

func buildExecution(in *repb.StoredExecution, inv *tables.Invocation) *Execution {
	return &Execution{
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
		User:                               inv.User,
		Host:                               inv.Host,
		Pattern:                            inv.Pattern,
		Role:                               inv.Role,
		BranchName:                         inv.BranchName,
		CommitSHA:                          inv.CommitSHA,
		RepoURL:                            inv.RepoURL,
		Command:                            inv.Command,
		Success:                            inv.Success,
		InvocationStatus:                   inv.InvocationStatus,
	}
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
	inv := ToInvocationFromPrimaryDB(ti)
	if err := h.insertWithRetrier(ctx, inv.TableName(), 1, inv); err != nil {
		return status.UnavailableErrorf("failed to insert invocation (invocation_id = %q), err: %s", ti.InvocationID, err)
	}
	return nil
}

func (h *DBHandle) FlushExecutionStats(ctx context.Context, ti *tables.Invocation, executions []*repb.StoredExecution) error {
	entries := make([]*Execution, 0, len(executions))
	for _, e := range executions {
		entries = append(entries, buildExecution(e, ti))
	}
	num := len(entries)
	if err := h.insertWithRetrier(ctx, (&Execution{}).TableName(), num, &entries); err != nil {
		return status.UnavailableErrorf("failed to insert %d execution(s) for invocation (invocation_id = %q), err: %s", num, ti.InvocationID, err)
	}
	return nil
}

func runMigrations(gdb *gorm.DB) error {
	log.Info("Auto-migrating clickhouse DB")
	if clusterOpts := getTableClusterOption(); clusterOpts != "" {
		gdb = gdb.Set("gorm:table_cluster_options", clusterOpts)
	}
	for _, t := range GetAllTables() {
		gdb = gdb.Set("gorm:table_options", t.TableOptions())
		if err := gdb.AutoMigrate(t); err != nil {
			return err
		}
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
		if err := runMigrations(db); err != nil {
			return err
		}
	}

	dbh := &DBHandle{
		db: db,
	}

	env.SetOLAPDBHandle(dbh)
	return nil
}

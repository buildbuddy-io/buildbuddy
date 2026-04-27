package schema

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gorm.io/gorm"
)

var (
	// {installation}, {cluster}, {shard}, {replica} are macros provided by
	// Altinity/clickhouse-operator; {database}, {table} are macros provided by clickhouse.
	dataReplicationEnabled = flag.Bool("olap_database.enable_data_replication", false, "If true, data replication is enabled.")
	zooPath                = flag.String("olap_database.zoo_path", "/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}", "The path to the table name in zookeeper, used to set up data replication")
	replicaName            = flag.String("olap_database.replica_name", "{replica}", "The replica name of the table in zookeeper")
	clusterName            = flag.String("olap_database.cluster_name", "{cluster}", "The cluster name of the database")
)

const (
	projectionCommits = "projection_commits"
)

// Making a new table? Please make sure you:
// 1) Add your table in getAllTables()
// 2) Add the table in clickhouse_test.go TestSchemaInSync
// 3) Make sure all the fields in the corresponding Table definition in tables.go
// are present in clickhouse Table definition or in ExcludedFields()
type Table interface {
	TableName() string
	TableOptions(clickhouseVersion string) string
	// Fields that are in the primary DB Table schema; but not in the clickhouse schema.
	ExcludedFields() []string
	// Fields that are in the clickhouse Table schema; but not in the primary DB Table Schema.
	AdditionalFields() []string
}

func getAllTables() []Table {
	tbls := []Table{
		&Invocation{},
		&Execution{},
		&TestTargetStatus{},
		&AuditLog{},
		&RawUsage{},
	}
	return tbls
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
	TotalUncachedActionExecUsec       int64
	DownloadThroughputBytesPerSecond  int64
	UploadThroughputBytesPerSecond    int64
	DownloadOutputsOption             int64
	UploadLocalResultsEnabled         bool
	RemoteExecutionEnabled            bool
	Tags                              []string `gorm:"type:Array(String);"`
	RunID                             string
	ParentRunID                       string
	RunStatus                         int64
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

func (i *Invocation) TableOptions(clickhouseVersion string) string {
	// Note: the sorting key need to be able to uniquely identify the invocation.
	// ReplacingMergeTree will remove entries with the same sorting key in the background.
	return fmt.Sprintf("ENGINE=%s ORDER BY (group_id, updated_at_usec, invocation_uuid)", getEngine()) +
		" PARTITION BY toYYYYMM(toDateTime(intDiv(updated_at_usec, 1000000), 'UTC'))"
}

type Execution struct {
	// Sort keys
	GroupID        string `gorm:"codec:ZSTD(1)"`
	UpdatedAtUsec  int64  `gorm:"codec:DoubleDelta,ZSTD(1)"`
	InvocationUUID string `gorm:"codec:ZSTD(1)"`
	ExecutionID    string `gorm:"codec:ZSTD(1)"`

	// Resource-name components split out of execution_id.
	// ActionDigest holds raw hash bytes (incompressible) — default codec.
	InstanceName     string `gorm:"codec:ZSTD(1)"`
	ExecutionUUID    string `gorm:"type:UUID"`
	Compressor       string `gorm:"type:LowCardinality(String)"`
	DigestFunction   string `gorm:"type:LowCardinality(String)"`
	ActionDigestHash string
	ActionDigestSize uint32 `gorm:"codec:T64,ZSTD(1)"`

	// Type from tables.InvocationExecution
	InvocationLinkType int8   `gorm:"codec:T64,ZSTD(1)"`
	CreatedAtUsec      int64  `gorm:"codec:DoubleDelta,ZSTD(1)"`
	UserID             string `gorm:"codec:ZSTD(1)"`
	Worker             string `gorm:"codec:ZSTD(1)"`
	ExecutorHostname   string `gorm:"codec:ZSTD(1)"`
	ClientIP           string `gorm:"codec:ZSTD(1)"`

	// Executor metadata
	SelfHosted bool
	Region     string `gorm:"type:LowCardinality(String)"`

	Stage int64 `gorm:"codec:T64,ZSTD(1)"`

	// RequestMetadata
	TargetLabel    string `gorm:"codec:ZSTD(1)"`
	ActionMnemonic string `gorm:"codec:ZSTD(1)"`

	// IOStats
	FileDownloadCount        int64 `gorm:"codec:T64,ZSTD(1)"`
	FileDownloadSizeBytes    int64 `gorm:"codec:T64,ZSTD(1)"`
	FileDownloadDurationUsec int64 `gorm:"codec:T64,ZSTD(1)"`
	FileUploadCount          int64 `gorm:"codec:T64,ZSTD(1)"`
	FileUploadSizeBytes      int64 `gorm:"codec:T64,ZSTD(1)"`
	FileUploadDurationUsec   int64 `gorm:"codec:T64,ZSTD(1)"`

	// UsageStats
	PeakMemoryBytes        int64 `gorm:"codec:T64,ZSTD(1)"`
	CPUNanos               int64 `gorm:"codec:T64,ZSTD(1)"`
	DiskBytesRead          int64 `gorm:"codec:T64,ZSTD(1)"`
	DiskBytesWritten       int64 `gorm:"codec:T64,ZSTD(1)"`
	DiskReadOperations     int64 `gorm:"codec:T64,ZSTD(1)"`
	DiskWriteOperations    int64 `gorm:"codec:T64,ZSTD(1)"`
	NetworkBytesSent       int64 `gorm:"codec:T64,ZSTD(1)"`
	NetworkBytesReceived   int64 `gorm:"codec:T64,ZSTD(1)"`
	NetworkPacketsSent     int64 `gorm:"codec:T64,ZSTD(1)"`
	NetworkPacketsReceived int64 `gorm:"codec:T64,ZSTD(1)"`

	// UsageStats - Linux PSI stats
	CPUPressureSomeStallUsec    int64 `gorm:"codec:T64,ZSTD(1)"`
	CPUPressureFullStallUsec    int64 `gorm:"codec:T64,ZSTD(1)"`
	MemoryPressureSomeStallUsec int64 `gorm:"codec:T64,ZSTD(1)"`
	MemoryPressureFullStallUsec int64 `gorm:"codec:T64,ZSTD(1)"`
	IOPressureSomeStallUsec     int64 `gorm:"codec:T64,ZSTD(1)"`
	IOPressureFullStallUsec     int64 `gorm:"codec:T64,ZSTD(1)"`

	// Task sizing
	EstimatedMemoryBytes          int64   `gorm:"codec:T64,ZSTD(1)"`
	EstimatedMilliCPU             int64   `gorm:"codec:T64,ZSTD(1)"`
	EstimatedFreeDiskBytes        int64   `gorm:"codec:T64,ZSTD(1)"`
	RequestedComputeUnits         float64 `gorm:"codec:ZSTD(1)"`
	RequestedMemoryBytes          int64   `gorm:"codec:T64,ZSTD(1)"`
	RequestedMilliCPU             int64   `gorm:"codec:T64,ZSTD(1)"`
	RequestedFreeDiskBytes        int64   `gorm:"codec:T64,ZSTD(1)"`
	PreviousMeasuredMemoryBytes   int64   `gorm:"codec:T64,ZSTD(1)"`
	PreviousMeasuredMilliCPU      int64   `gorm:"codec:T64,ZSTD(1)"`
	PreviousMeasuredFreeDiskBytes int64   `gorm:"codec:T64,ZSTD(1)"`
	PredictedMemoryBytes          int64   `gorm:"codec:T64,ZSTD(1)"`
	PredictedMilliCPU             int64   `gorm:"codec:T64,ZSTD(1)"`
	PredictedFreeDiskBytes        int64   `gorm:"codec:T64,ZSTD(1)"`

	// ExecutedActionMetadata (in addition to Worker above)
	QueuedTimestampUsec                int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`
	WorkerStartTimestampUsec           int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`
	WorkerCompletedTimestampUsec       int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`
	InputFetchStartTimestampUsec       int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`
	InputFetchCompletedTimestampUsec   int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`
	ExecutionStartTimestampUsec        int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`
	ExecutionCompletedTimestampUsec    int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`
	OutputUploadStartTimestampUsec     int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`
	OutputUploadCompletedTimestampUsec int64 `gorm:"codec:DoubleDelta,ZSTD(1)"`

	StatusCode int32 `gorm:"codec:T64,ZSTD(1)"`
	ExitCode   int32 `gorm:"codec:T64,ZSTD(1)"`

	CachedResult    bool
	DoNotCache      bool
	SkipCacheLookup bool

	ExecutionPriority      int32  `gorm:"codec:T64,ZSTD(1)"`
	RequestedIsolationType string `gorm:"type:LowCardinality(String)"`
	EffectiveIsolationType string `gorm:"type:LowCardinality(String)"` // This values comes from the executor
	RequestedPool          string `gorm:"codec:ZSTD(1)"`
	EffectivePool          string `gorm:"codec:ZSTD(1)"`

	// Runner metadata
	RunnerID            string `gorm:"codec:ZSTD(1)"`
	RunnerTaskNumber    int64  `gorm:"codec:T64,ZSTD(1)"`
	PlatformHash        string `gorm:"codec:ZSTD(1)"`
	PersistentWorkerKey string `gorm:"codec:ZSTD(1)"`

	RequestedTimeoutUsec int64 `gorm:"codec:T64,ZSTD(1)"`
	EffectiveTimeoutUsec int64 `gorm:"codec:T64,ZSTD(1)"`

	Experiments []string `gorm:"type:Array(LowCardinality(String))"`

	// Long string fields
	OutputPath     string `gorm:"codec:ZSTD(1)"`
	StatusMessage  string `gorm:"codec:ZSTD(1)"`
	CommandSnippet string `gorm:"codec:ZSTD(1)"`

	// Fields from Invocations
	User             string `gorm:"codec:ZSTD(1)"`
	Host             string `gorm:"codec:ZSTD(1)"`
	Pattern          string `gorm:"codec:ZSTD(1)"`
	Role             string `gorm:"type:LowCardinality(String)"`
	BranchName       string `gorm:"codec:ZSTD(1)"`
	CommitSHA        string `gorm:"codec:ZSTD(1)"`
	RepoURL          string `gorm:"codec:ZSTD(1)"`
	Command          string `gorm:"codec:ZSTD(1)"`
	InvocationStatus int64  `gorm:"codec:T64,ZSTD(1)"`
	Success          bool
	Tags             []string `gorm:"type:Array(String);codec:ZSTD(1)"`
}

func (e *Execution) TableName() string {
	return "Executions"
}

func (e *Execution) TableOptions(clickhouseVersion string) string {
	return fmt.Sprintf("ENGINE=%s ORDER BY (group_id, updated_at_usec, invocation_uuid, execution_uuid)", getEngine()) +
		" PARTITION BY toYYYYMM(toDateTime(intDiv(updated_at_usec, 1000000), 'UTC'))"
}

func (e *Execution) ExcludedFields() []string {
	return []string{
		"InvocationID",
		"Perms",
		"SerializedOperation",
		"SerializedStatusDetails",
	}
}

func (e *Execution) AdditionalFields() []string {
	return []string{
		"InvocationUUID",
		"InstanceName",
		"ExecutionUUID",
		"Compressor",
		"DigestFunction",
		"ActionDigestHash",
		"ActionDigestSize",
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
		"Tags",
		"OutputPath",
		"TargetLabel",
		"ActionMnemonic",
		"DiskBytesRead",
		"DiskBytesWritten",
		"DiskReadOperations",
		"DiskWriteOperations",
		"NetworkBytesSent",
		"NetworkBytesReceived",
		"NetworkPacketsSent",
		"NetworkPacketsReceived",
		"CPUPressureSomeStallUsec",
		"CPUPressureFullStallUsec",
		"MemoryPressureSomeStallUsec",
		"MemoryPressureFullStallUsec",
		"IOPressureSomeStallUsec",
		"IOPressureFullStallUsec",
		"EstimatedFreeDiskBytes",
		"RequestedComputeUnits",
		"RequestedMemoryBytes",
		"RequestedMilliCPU",
		"RequestedFreeDiskBytes",
		"PreviousMeasuredMemoryBytes",
		"PreviousMeasuredMilliCPU",
		"PreviousMeasuredFreeDiskBytes",
		"PredictedMemoryBytes",
		"PredictedMilliCPU",
		"PredictedFreeDiskBytes",
		"SkipCacheLookup",
		"ExecutionPriority",
		"RequestedIsolationType",
		"EffectiveIsolationType",
		"RequestedPool",
		"EffectivePool",
		"RunnerID",
		"RunnerTaskNumber",
		"PlatformHash",
		"PersistentWorkerKey",
		"RequestedTimeoutUsec",
		"EffectiveTimeoutUsec",
		"Region",
		"SelfHosted",
		"ExecutorHostname",
		"Experiments",
		"ClientIP",
	}
}

// TestTargetStatus represents the status of a target, the target info and
// invocation details
type TestTargetStatus struct {
	// Sort Keys; and the order of the following fields match TableOptions().
	GroupID        string
	RepoURL        string
	CommitSHA      string
	Label          string
	InvocationUUID string

	RuleType      string
	UserID        string
	TargetType    int32
	TestSize      int32
	Status        int32
	Cached        bool
	StartTimeUsec int64
	DurationUsec  int64

	// The following fields are from Invocation.
	BranchName string
	Role       string
	Command    string
	// The start time of the invocation. Note: for backfilled records, this field
	// uses Invocation.CreatedAtUsec because StartTimeUsec is not saved for the
	// invocation.
	InvocationStartTimeUsec int64
}

func (t *TestTargetStatus) ExcludedFields() []string {
	return []string{}
}

func (t *TestTargetStatus) AdditionalFields() []string {
	return []string{}
}

func (t *TestTargetStatus) TableName() string {
	return "TestTargetStatuses"
}

func (t *TestTargetStatus) TableOptions(clickhouseVersion string) string {
	options := fmt.Sprintf("ENGINE=%s ORDER BY (group_id, repo_url, commit_sha, label, invocation_uuid)", getEngine()) +
		" PARTITION BY toYYYYMM(toDateTime(intDiv(invocation_start_time_usec, 1000000), 'UTC'))"
	if clickhouseVersion > "24.8" {
		// Clickhouse 24.8 added a table setting, deduplicate_merge_projection_mode,
		// that is required when adding projections with on tables with merge engines.
		options += " SETTINGS deduplicate_merge_projection_mode = 'rebuild'"
	}
	return options
}

type AuditLog struct {
	AuditLogID    string
	GroupID       string
	EventTimeUsec int64

	AuthAPIKeyID    string
	AuthAPIKeyLabel string

	AuthUserID    string
	AuthUserEmail string

	ClientIP string

	ResourceID   string
	ResourceName string
	ResourceType uint8

	Action uint8

	Request string
}

func (i *AuditLog) ExcludedFields() []string {
	return []string{}
}

func (i *AuditLog) AdditionalFields() []string {
	return []string{}
}

func (i *AuditLog) TableName() string {
	return "AuditLogs"
}

func (i *AuditLog) TableOptions(clickhouseVersion string) string {
	return fmt.Sprintf("ENGINE=%s ORDER BY (group_id, event_time_usec, audit_log_id)", getEngine())
}

// RawUsage contains usage data which may potentially contain duplicate rows.
// Use the Usage view to get unique usage counts.
//
// See http://go/usage-v2 for details.
type RawUsage struct {
	// GroupID is the BuildBuddy group ID to which the usage is attributed.
	GroupID string `gorm:"type:String"`

	// SKU is the usage counter type.
	SKU sku.SKU `gorm:"type:LowCardinality(String)"`

	// Labels contains additional labels used to further qualify the SKU.
	// This should only be used in cases where there may be a large number of
	// possible dimensions (too many to be represented as a SKU) but the
	// frequency of usage for each combination of dimensions is expected to be
	// relatively low.
	Labels map[sku.LabelName]sku.LabelValue `gorm:"type:Map(LowCardinality(String), LowCardinality(String))"`

	// PeriodStart is the start of the period during which the usage occurred.
	// Currently, usage is collected in 1-minute intervals.
	PeriodStart time.Time `gorm:"type:DateTime64(6, 'UTC')"`

	// BufferID uniquely identifies the storage location where this usage row
	// was buffered before being flushed to ClickHouse, so that if the buffered
	// rows are flushed more than once (e.g. due to transient errors), the
	// flushed usage data is deduplicated when using a FINAL query over the
	// table. Currently, we buffer in Redis, in each cluster where the app is
	// located. So an appropriate value here might be "<cluster-name>:redis".
	BufferID string `gorm:"type:LowCardinality(String)"`

	// Count is the number of units of usage measured.
	Count int64 `gorm:"type:Int64"`
}

func (u *RawUsage) TableName() string {
	return "RawUsage"
}

func (i *RawUsage) TableOptions(clickhouseVersion string) string {
	return "ENGINE=" + getEngine() +
		" ORDER BY (group_id, period_start, sku, labels, buffer_id)" +
		// When using FINAL to deduplicate, partitioning by month allows
		// the deduplication to be done in parallel for each partition.
		// See https://clickhouse.com/docs/guides/replacing-merge-tree#partitioning-and-merging-across-partitions
		//
		" PARTITION BY toYYYYMM(period_start)"
}

func (i *RawUsage) ExcludedFields() []string {
	return nil
}

func (i *RawUsage) AdditionalFields() []string {
	return nil
}

// Usage represents a row queried from the Usage view, which is the deduped
// and aggregated version of RawUsage.
type Usage struct {
	GroupID     string
	PeriodStart time.Time
	SKU         sku.SKU
	Labels      map[sku.LabelName]sku.LabelValue `gorm:"type:Map(LowCardinality(String), LowCardinality(String))"`
	Count       int64
}

// hasProjection checks whether a projection exist in the clickhouse
// schema.
// gorm-clickhouse doesn't support migration projection.
func hasProjection(db *gorm.DB, table Table, projectionName string) (bool, error) {
	currentDatabase := db.Migrator().CurrentDatabase()

	showCreateTableSQL := fmt.Sprintf("SHOW CREATE TABLE %s.%s", currentDatabase, table.TableName())
	var createStmt string
	if err := db.Raw(showCreateTableSQL).Row().Scan(&createStmt); err != nil {
		return false, err
	}

	projections := extractProjectionNamesFromCreateStmt(createStmt)

	_, ok := projections[projectionName]

	return ok, nil
}

// addProjectionIfNotExists checks whether a projection exist in the clickhouse
// schema; if not, add the projection.
// gorm-clickhouse doesn't support migration projection.
func addProjectionIfNotExist(db *gorm.DB, table Table, projectionName string, query string) error {
	hasProjection, err := hasProjection(db, table, projectionName)
	if err != nil {
		return status.InternalErrorf("failed to check whether projection %q exists: %s", projectionName, err)
	}
	if hasProjection {
		return nil
	}
	projectionStmt := fmt.Sprintf("ALTER TABLE %s ADD PROJECTION %s (%s)", table.TableName(), projectionName, query)
	return db.Exec(projectionStmt).Error
}

const (
	beforeCreateBody int = iota
	inCreateBody
	inProjection
	afterCreateBody
)

// adapted from https://github.com/go-gorm/clickhouse/blob/master/migrator.go
func extractProjectionNamesFromCreateStmt(createStmt string) map[string]struct{} {
	names := make(map[string]struct{})
	scanner := bufio.NewScanner(strings.NewReader(createStmt))
	state := beforeCreateBody
	for scanner.Scan() && state < afterCreateBody {
		line := scanner.Text()
		line = strings.TrimSpace(line)
		switch state {
		case beforeCreateBody:
			if strings.HasPrefix(line, "(") {
				state = inCreateBody
			}
		case inProjection:
			if strings.HasPrefix(line, ")") {
				state = inCreateBody
			}
		case inCreateBody:
			if strings.HasPrefix(line, ")") {
				state = afterCreateBody
				continue
			}
			if after, ok := strings.CutPrefix(line, "PROJECTION "); ok {
				line = after
				elems := strings.Split(line, " ")
				if len(elems) > 0 {
					names[elems[0]] = struct{}{}
				}
				state = inProjection
			}
		}
	}
	return names
}

func RunMigrations(gdb *gorm.DB) error {
	versionStr := ""
	if err := gdb.Raw("select version()").Scan(&versionStr).Error; err != nil {
		log.Warningf("Failed to get clickhouse version: %v", err)
	}
	log.Info("Auto-migrating clickhouse DB")
	if clusterOpts := getTableClusterOption(); clusterOpts != "" {
		gdb = gdb.Set("gorm:table_cluster_options", clusterOpts)
	}
	for _, t := range getAllTables() {
		gdb = gdb.Set("gorm:table_options", t.TableOptions(versionStr))
		if err := gdb.AutoMigrate(t); err != nil {
			return err
		}
	}
	// Add Projection/
	projectionQuery := `select group_id, repo_url, commit_sha,
	   max(invocation_start_time_usec) as latest_created_at_usec
	   group by group_id, repo_url, commit_sha`
	err := addProjectionIfNotExist(gdb, &TestTargetStatus{}, projectionCommits, projectionQuery)
	if err != nil {
		return status.InternalErrorf("failed to add projection %q: %s", projectionCommits, err)
	}
	return nil
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
		TotalUncachedActionExecUsec:       ti.TotalUncachedActionExecUsec,
		DownloadThroughputBytesPerSecond:  ti.DownloadThroughputBytesPerSecond,
		UploadThroughputBytesPerSecond:    ti.UploadThroughputBytesPerSecond,
		DownloadOutputsOption:             ti.DownloadOutputsOption,
		UploadLocalResultsEnabled:         ti.UploadLocalResultsEnabled,
		RemoteExecutionEnabled:            ti.RemoteExecutionEnabled,
		Tags:                              invocation_format.ConvertDBTagsToOLAP(ti.Tags),
		RunID:                             ti.RunID,
		ParentRunID:                       ti.ParentRunID,
		RunStatus:                         ti.RunStatus,
	}
}

package schema

import (
	"bufio"
	"database/sql"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"gorm.io/gorm"

	gormschema "gorm.io/gorm/schema"
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

	// TenantGroupIDSetting is the custom ClickHouse setting that holds the
	// group ID of a tenant query. The tenant row policy compares group_id
	// against it, so ClickHouse fails tenant queries that do not set it. The
	// server's custom_settings_prefixes must include the "SQL_" prefix.
	TenantGroupIDSetting = "SQL_group_id"

	// tenantAccessName is the name of the role, row policy, settings profile
	// and quota that EnsureTenantAccess creates.
	tenantAccessName = "bb_tenant_query"
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
	// Fields that tenant queries may read (see EnsureTenantAccess). Tenant
	// queries cannot read any other field, including new fields, until we
	// list the field here.
	TenantFields() []string
}

// View represents a ClickHouse "normal view", which is essentially just a saved
// query.
//
// To define a new view, declare a new struct implementing this interface, and
// add it to [getAllViews].
//
// For more on views, see:
// https://clickhouse.com/docs/sql-reference/statements/create/view
type View interface {
	ViewName() string
	ViewQuery() string
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

func getAllViews() []View {
	return []View{
		&Usage{},
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

	// Git fetch stats reported by the remote runner (i.e. Workflows or remote
	// bazel), if any.
	GitFetchTotalBytes   int64
	GitFetchDurationUsec int64
	GitFetchRetryCount   int64
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

func (i *Invocation) TenantFields() []string {
	return []string{
		"GroupID",
		"UpdatedAtUsec",
		"CreatedAtUsec",
		"InvocationUUID",
		"Role",
		"User",
		"Host",
		"CommitSHA",
		"BranchName",
		"Command",
		"BazelExitCode",
		"UserID",
		"Pattern",
		"InvocationStatus",
		"Attempt",
		"ActionCount",
		"RepoURL",
		"DurationUsec",
		"Success",
		"ActionCacheHits",
		"ActionCacheMisses",
		"ActionCacheUploads",
		"CasCacheHits",
		"CasCacheMisses",
		"CasCacheUploads",
		"TotalDownloadSizeBytes",
		"TotalUploadSizeBytes",
		"TotalDownloadTransferredSizeBytes",
		"TotalUploadTransferredSizeBytes",
		"TotalDownloadUsec",
		"TotalUploadUsec",
		"TotalCachedActionExecUsec",
		"TotalUncachedActionExecUsec",
		"DownloadThroughputBytesPerSecond",
		"UploadThroughputBytesPerSecond",
		"DownloadOutputsOption",
		"UploadLocalResultsEnabled",
		"RemoteExecutionEnabled",
		"Tags",
		"RunID",
		"ParentRunID",
		"RunStatus",
		"GitFetchTotalBytes",
		"GitFetchDurationUsec",
		"GitFetchRetryCount",
	}
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
	OS         string `gorm:"type:LowCardinality(String)"`
	Arch       string `gorm:"type:LowCardinality(String)"`

	Stage int64 `gorm:"codec:T64,ZSTD(1)"`

	// RequestMetadata
	TargetLabel    string `gorm:"codec:ZSTD(1)"`
	ActionMnemonic string `gorm:"codec:ZSTD(1)"`
	// ConfigurationID identifies the configuration in which the target was built.
	ConfigurationID string `gorm:"codec:ZSTD(1)"`

	// Test metadata
	TestSize        string `gorm:"type:LowCardinality(String)"`
	TestShardIndex  uint32 `gorm:"codec:T64,ZSTD(1)"`
	TestTotalShards uint32 `gorm:"codec:T64,ZSTD(1)"`

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

	ExecutionPriority int32 `gorm:"codec:T64,ZSTD(1)"`

	// Platform properties
	RequestedIsolationType string `gorm:"type:LowCardinality(String)"`
	EffectiveIsolationType string `gorm:"type:LowCardinality(String)"` // This values comes from the executor
	RequestedPool          string `gorm:"codec:ZSTD(1)"`
	EffectivePool          string `gorm:"codec:ZSTD(1)"`
	RecycleRunner          bool

	// Runner metadata
	RunnerID            string `gorm:"codec:ZSTD(1)"`
	RunnerTaskNumber    int64  `gorm:"codec:T64,ZSTD(1)"`
	PlatformHash        string `gorm:"codec:ZSTD(1)"`
	PersistentWorkerKey string `gorm:"codec:ZSTD(1)"`

	RequestedTimeoutUsec int64 `gorm:"codec:T64,ZSTD(1)"`
	EffectiveTimeoutUsec int64 `gorm:"codec:T64,ZSTD(1)"`

	// Snapshot save stats reported after the executions is marked as completed.
	SnapshotSavedLocally  bool
	SnapshotSavedRemotely bool
	SnapshotIsDiff        bool
	SnapshotSavedBytes    int64 `gorm:"codec:T64,ZSTD(1)"`
	PauseDurationUsec     int64 `gorm:"codec:T64,ZSTD(1)"`

	// Guest VM boot wait timings. Only set for executions that booted a fresh
	// firecracker VM.
	VMDockerdWaitDurationUsec int64 `gorm:"codec:T64,ZSTD(1)"`
	VMDnsWaitDurationUsec     int64 `gorm:"codec:T64,ZSTD(1)"`
	VMExecInitDurationUsec    int64 `gorm:"codec:T64,ZSTD(1)"`
	// Time the executor spent dialing the guest VM's vmexec server. Set for
	// every firecracker execution, not just fresh boots.
	VMExecDialDurationUsec int64 `gorm:"codec:T64,ZSTD(1)"`

	// Disk usage of the task's workspace (buildroot), measured after the task
	// finishes. Only populated when executor.workspace.measure_disk_usage is
	// enabled.
	BuildrootDiskUsageBytes int64 `gorm:"codec:T64,ZSTD(1)"`

	// Deprecated: use ExperimentVariants instead.
	//
	// TODO(bduffany): Drop this column after migrating executors to use expflag
	Experiments []string `gorm:"type:Array(LowCardinality(String))"`

	// Maps experiment flag names to the variant selected for the execution.
	ExperimentVariants map[string]string `gorm:"type:Map(LowCardinality(String), LowCardinality(String))"`

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
		"ExecutionID",
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
		"ConfigurationID",
		"TestSize",
		"TestShardIndex",
		"TestTotalShards",
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
		"RecycleRunner",
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
		"OS",
		"Arch",
		"SnapshotSavedLocally",
		"SnapshotSavedRemotely",
		"SnapshotIsDiff",
		"SnapshotSavedBytes",
		"PauseDurationUsec",
		"VMDockerdWaitDurationUsec",
		"VMDnsWaitDurationUsec",
		"VMExecInitDurationUsec",
		"VMExecDialDurationUsec",
		"BuildrootDiskUsageBytes",
		"ExecutorHostname",
		"Experiments",
		"ExperimentVariants",
		"ClientIP",
	}
}

func (e *Execution) TenantFields() []string {
	return []string{
		"GroupID",
		"UpdatedAtUsec",
		"InvocationUUID",
		"InstanceName",
		"ExecutionUUID",
		"Compressor",
		"DigestFunction",
		"ActionDigestHash",
		"ActionDigestSize",
		"InvocationLinkType",
		"CreatedAtUsec",
		"UserID",
		"SelfHosted",
		"Region",
		"OS",
		"Arch",
		"Stage",
		"TargetLabel",
		"ActionMnemonic",
		"ConfigurationID",
		"TestSize",
		"TestShardIndex",
		"TestTotalShards",
		"FileDownloadCount",
		"FileDownloadSizeBytes",
		"FileDownloadDurationUsec",
		"FileUploadCount",
		"FileUploadSizeBytes",
		"FileUploadDurationUsec",
		"PeakMemoryBytes",
		"CPUNanos",
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
		"EstimatedMemoryBytes",
		"EstimatedMilliCPU",
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
		"QueuedTimestampUsec",
		"WorkerStartTimestampUsec",
		"WorkerCompletedTimestampUsec",
		"InputFetchStartTimestampUsec",
		"InputFetchCompletedTimestampUsec",
		"ExecutionStartTimestampUsec",
		"ExecutionCompletedTimestampUsec",
		"OutputUploadStartTimestampUsec",
		"OutputUploadCompletedTimestampUsec",
		"StatusCode",
		"ExitCode",
		"CachedResult",
		"DoNotCache",
		"SkipCacheLookup",
		"ExecutionPriority",
		"RequestedIsolationType",
		"EffectiveIsolationType",
		"RequestedPool",
		"EffectivePool",
		"RecycleRunner",
		"RunnerTaskNumber",
		"RequestedTimeoutUsec",
		"EffectiveTimeoutUsec",
		"SnapshotSavedLocally",
		"SnapshotSavedRemotely",
		"SnapshotIsDiff",
		"SnapshotSavedBytes",
		"PauseDurationUsec",
		"VMDockerdWaitDurationUsec",
		"VMDnsWaitDurationUsec",
		"VMExecInitDurationUsec",
		"VMExecDialDurationUsec",
		"BuildrootDiskUsageBytes",
		"StatusMessage",
		"CommandSnippet",
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
		"Tags",
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

	RuleType               string
	UserID                 string
	TargetType             int32
	TestSize               int32
	Status                 int32
	Cached                 bool
	StartTimeUsec          int64
	DurationUsec           int64
	TestTimeoutUsec        int64
	MaxAttemptDurationUsec int64

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

func (t *TestTargetStatus) TenantFields() []string {
	return []string{
		"GroupID",
		"RepoURL",
		"CommitSHA",
		"Label",
		"InvocationUUID",
		"RuleType",
		"UserID",
		"TargetType",
		"TestSize",
		"Status",
		"Cached",
		"StartTimeUsec",
		"DurationUsec",
		"TestTimeoutUsec",
		"MaxAttemptDurationUsec",
		"BranchName",
		"Role",
		"Command",
		"InvocationStartTimeUsec",
	}
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

func (i *AuditLog) TenantFields() []string {
	return nil
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
	// relatively low. Since RawUsage.labels is part of the ReplacingMergeTree
	// ORDER BY key, callers must write labels with a deterministic key order so
	// FINAL deduplication works reliably.
	Labels *orderedmap.Map[sku.LabelName, sku.LabelValue] `gorm:"type:Map(LowCardinality(String), LowCardinality(String))"`

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

func (i *RawUsage) TenantFields() []string {
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

func (u *Usage) ViewName() string {
	return "Usage"
}

func (u *Usage) ViewQuery() string {
	return "SELECT group_id, period_start, sku, labels, SUM(count) AS count " +
		"FROM RawUsage FINAL GROUP BY group_id, period_start, sku, labels"
}

func execCreateView(gdb *gorm.DB, view View, replace, ifNotExists bool) error {
	q := "CREATE"
	if replace {
		q += " OR REPLACE"
	}
	q += " VIEW"
	if ifNotExists {
		q += " IF NOT EXISTS"
	}
	q += fmt.Sprintf(" %q", view.ViewName())
	if clusterOption := getTableClusterOption(); clusterOption != "" {
		q += " " + clusterOption
	}
	q += " AS " + view.ViewQuery()
	return gdb.Exec(q).Error
}

func ensureView(gdb *gorm.DB, view View) error {
	expectedQuery := view.ViewQuery()
	existingView, err := lookupExistingView(gdb, view.ViewName())
	if err != nil {
		return err
	}
	if existingView == nil {
		// Another app may create the view after this app observes it as
		// missing, so use IF NOT EXISTS to make concurrent startup harmless.
		return execCreateView(gdb, view, false /*=replace*/, true /*=ifNotExists*/)
	}
	if sameViewQuery(existingView.SelectQuery, expectedQuery, existingView.Database) {
		return nil
	}
	return execCreateView(gdb, view, true /*=replace*/, false /*=ifNotExists*/)
}

type viewMetadata struct {
	// Database is the DB where the view is defined.
	Database string `gorm:"column:current_database"`

	// SelectQuery is the saved SELECT query for the view. We compare it against
	// ViewQuery to decide whether startup migrations need to replace the view.
	SelectQuery string `gorm:"column:as_select"`

	// Engine is the table engine - should always be "View" for views.
	Engine string `gorm:"column:engine"`
}

func lookupExistingView(gdb *gorm.DB, viewName string) (*viewMetadata, error) {
	row := &viewMetadata{}
	err := gdb.Raw(`
		SELECT currentDatabase() AS current_database, engine, as_select
		FROM system.tables
		WHERE database = currentDatabase() AND name = ?
	`, viewName).Row().Scan(&row.Database, &row.Engine, &row.SelectQuery)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if row.Engine != "View" {
		return nil, status.FailedPreconditionErrorf("%q exists but has ClickHouse engine %q, not View", viewName, row.Engine)
	}
	return row, nil
}

func sameViewQuery(a, b, currentDatabase string) bool {
	return normalizeViewQuery(a, currentDatabase) == normalizeViewQuery(b, currentDatabase)
}

func normalizeViewQuery(query, currentDatabase string) string {
	query = strings.ReplaceAll(query, "`", "")
	if currentDatabase != "" {
		query = strings.ReplaceAll(query, currentDatabase+".", "")
	}
	return strings.ToLower(strings.Join(strings.Fields(query), " "))
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
	for _, v := range getAllViews() {
		if err := ensureView(gdb, v); err != nil {
			return status.WrapErrorf(err, "ensure view %q", v.ViewName())
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

// TenantAccess configures the ClickHouse user that runs tenant queries, and
// the limits that ClickHouse enforces on those queries.
type TenantAccess struct {
	// User is the name of the ClickHouse user that runs tenant queries.
	User string
	// PasswordSHA256 is the hex-encoded SHA-256 hash of the user's password.
	PasswordSHA256 string

	// MaxExecutionTime is the longest that a query may run.
	MaxExecutionTime time.Duration
	// MaxThreads is the maximum number of threads that a query may use, which
	// also limits how many CPU cores it can use.
	MaxThreads int64
	// MaxMemoryBytes is the maximum amount of memory that a query may use.
	MaxMemoryBytes int64
	// MaxResultRows is the maximum number of rows that a query may return.
	MaxResultRows int64
	// MaxResultBytes is the maximum size of the result that a query may
	// return.
	MaxResultBytes int64
	// MaxConcurrentQueries is the maximum number of queries that may run at
	// the same time, counting the queries of every group.
	MaxConcurrentQueries int64

	// MaxQueriesPerHour is the maximum number of queries that each group may
	// run per hour.
	MaxQueriesPerHour int64
	// MaxExecutionTimePerHour is the maximum total time that the queries of
	// each group may run per hour.
	MaxExecutionTimePerHour time.Duration
}

// EnsureTenantAccess creates the ClickHouse user that runs tenant queries (see
// server/util/clickhouse/tenantquery), and limits what the user can read and
// how many resources its queries can use. Tenants write the SQL of these
// queries, so we rely on ClickHouse to enforce the limits instead of
// inspecting each query.
//
// We create the following for the current database:
//   - A role, which is the only role that we grant to the user. Everything
//     below applies to the role.
//   - A row policy on every table, which only lets a query read the rows whose
//     group_id equals the query's TenantGroupIDSetting setting.
//   - A settings profile, which limits each query.
//   - A quota, which limits each group. ClickHouse tracks usage separately for
//     each quota key, and tenant queries pass the group ID as the quota key.
//   - Grants to read the TenantFields of each table.
//
// The statements are idempotent, so we run them on every migration in order
// to apply changes to the TenantAccess and to TenantFields.
//
// Tenant isolation also depends on the following server settings, which
// ClickHouse's stock config.xml has:
//   - custom_settings_prefixes includes "SQL_", so that queries can set
//     TenantGroupIDSetting.
//   - select_from_system_db_requires_grant is true. Otherwise tenant queries
//     can read system.processes, which shows the queries of other groups.
//   - users_without_row_policies_can_read_rows is true. Otherwise the row
//     policy hides all rows from every other user, including the user of the
//     OLAP DB handle.
func EnsureTenantAccess(gdb *gorm.DB, access *TenantAccess) error {
	db := gdb.Migrator().CurrentDatabase()
	// By default, each ClickHouse server stores users, roles and policies in
	// its own local directory, so on a cluster we have to create them on every
	// server.
	onCluster := ""
	if clusterOption := getTableClusterOption(); clusterOption != "" {
		onCluster = " " + clusterOption
	}
	stmts := []string{"CREATE ROLE IF NOT EXISTS " + tenantAccessName + onCluster}
	for _, entity := range []struct{ kind, definition string }{
		{
			kind: "ROW POLICY",
			// The policy is restrictive so that ClickHouse applies it in
			// addition to every other policy. If it were permissive, then
			// adding another permissive policy for the role would let tenant
			// queries read the rows that match either policy.
			definition: "ON " + db + ".* AS RESTRICTIVE FOR SELECT" +
				" USING group_id = getSetting('" + TenantGroupIDSetting + "')",
		},
		{
			kind: "SETTINGS PROFILE",
			definition: "SETTINGS" +
				fmt.Sprintf(" max_execution_time = %f CONST,", access.MaxExecutionTime.Seconds()) +
				fmt.Sprintf(" max_threads = %d CONST,", access.MaxThreads) +
				fmt.Sprintf(" max_memory_usage = %d CONST,", access.MaxMemoryBytes) +
				fmt.Sprintf(" max_result_rows = %d CONST,", access.MaxResultRows) +
				fmt.Sprintf(" max_result_bytes = %d CONST,", access.MaxResultBytes) +
				fmt.Sprintf(" max_concurrent_queries_for_user = %d CONST,", access.MaxConcurrentQueries) +
				// Otherwise, ClickHouse keeps running a query after we close
				// the connection, which we do when the caller cancels the
				// request.
				" cancel_http_readonly_queries_on_client_close = 1 CONST",
		},
		{
			kind: "QUOTA",
			definition: "KEYED BY client_key FOR INTERVAL 1 hour MAX" +
				fmt.Sprintf(" queries = %d,", access.MaxQueriesPerHour) +
				fmt.Sprintf(" execution_time = %f", access.MaxExecutionTimePerHour.Seconds()),
		},
	} {
		// If the entity already exists, we alter it instead of replacing it.
		// This way the row policy applies to tenant queries at all times, and
		// the quota keeps the usage that ClickHouse has tracked for each
		// group.
		definition := onCluster + " " + entity.definition + " TO " + tenantAccessName
		stmts = append(stmts,
			"CREATE "+entity.kind+" IF NOT EXISTS "+tenantAccessName+definition,
			"ALTER "+entity.kind+" "+tenantAccessName+definition,
		)
	}
	// We create the user after the row policy, so that the user can never read
	// rows without it.
	identified := onCluster + " IDENTIFIED WITH sha256_hash BY '" + access.PasswordSHA256 + "'"
	stmts = append(stmts,
		"CREATE USER IF NOT EXISTS "+access.User+identified,
		"GRANT"+onCluster+" "+tenantAccessName+" TO "+access.User,
		// ClickHouse only accepts a default role that the user already has,
		// so this has to come after the grant.
		"ALTER USER "+access.User+identified+" DEFAULT ROLE "+tenantAccessName,
	)
	for _, stmt := range stmts {
		if err := gdb.Exec(stmt).Error; err != nil {
			// Only name the kind of statement, because some of the statements
			// contain the password hash.
			return fmt.Errorf("%s: %w", strings.Join(strings.Fields(stmt)[:2], " "), err)
		}
	}
	return reconcileTenantGrants(gdb, db, onCluster)
}

// reconcileTenantGrants grants the tenant role access to the TenantFields of
// each table, and revokes any other read access that the role has to the
// database, so that removing a field from TenantFields hides it from tenants.
func reconcileTenantGrants(gdb *gorm.DB, db, onCluster string) error {
	columns := map[string][]string{}
	for _, t := range getAllTables() {
		if len(t.TenantFields()) == 0 {
			continue
		}
		s, err := gormschema.Parse(t, &sync.Map{}, gdb.NamingStrategy)
		if err != nil {
			return fmt.Errorf("parse schema of %s: %w", t.TableName(), err)
		}
		for _, name := range t.TenantFields() {
			field, ok := s.FieldsByName[name]
			if !ok {
				return fmt.Errorf("%s has no field named %s", t.TableName(), name)
			}
			columns[t.TableName()] = append(columns[t.TableName()], field.DBName)
		}
	}

	var grants []struct {
		Table  sql.NullString
		Column sql.NullString
	}
	err := gdb.Raw(
		"SELECT `table`, `column` FROM system.grants WHERE role_name = ? AND access_type = 'SELECT' AND database = ?",
		tenantAccessName, db,
	).Scan(&grants).Error
	if err != nil {
		return fmt.Errorf("list grants: %w", err)
	}
	for _, grant := range grants {
		if grant.Column.Valid && slices.Contains(columns[grant.Table.String], grant.Column.String) {
			continue
		}
		// ClickHouse reports a NULL column for a grant on a whole table, and
		// also a NULL table for a grant on the whole database.
		privilege := "SELECT"
		if grant.Column.Valid {
			privilege = "SELECT(" + grant.Column.String + ")"
		}
		target := db + ".*"
		if grant.Table.Valid {
			target = db + "." + grant.Table.String
		}
		stmt := "REVOKE" + onCluster + " " + privilege + " ON " + target + " FROM " + tenantAccessName
		if err := gdb.Exec(stmt).Error; err != nil {
			return fmt.Errorf("revoke %s on %s: %w", privilege, target, err)
		}
	}
	for _, t := range getAllTables() {
		if len(columns[t.TableName()]) == 0 {
			continue
		}
		stmt := "GRANT" + onCluster + " SELECT(" + strings.Join(columns[t.TableName()], ", ") + ")" +
			" ON " + db + "." + t.TableName() + " TO " + tenantAccessName
		if err := gdb.Exec(stmt).Error; err != nil {
			return fmt.Errorf("grant select on %s: %w", t.TableName(), err)
		}
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
		GitFetchTotalBytes:                ti.GitFetchTotalBytes,
		GitFetchDurationUsec:              ti.GitFetchDurationUsec,
		GitFetchRetryCount:                ti.GitFetchRetryCount,
	}
}

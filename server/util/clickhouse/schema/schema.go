package schema

import (
	"encoding/hex"
	"flag"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
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

func getAllTables() []Table {
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

func RunMigrations(gdb *gorm.DB) error {
	log.Info("Auto-migrating clickhouse DB")
	if clusterOpts := getTableClusterOption(); clusterOpts != "" {
		gdb = gdb.Set("gorm:table_cluster_options", clusterOpts)
	}
	for _, t := range getAllTables() {
		gdb = gdb.Set("gorm:table_options", t.TableOptions())
		if err := gdb.AutoMigrate(t); err != nil {
			return err
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
		DownloadThroughputBytesPerSecond:  ti.DownloadThroughputBytesPerSecond,
		UploadThroughputBytesPerSecond:    ti.UploadThroughputBytesPerSecond,
	}
}

package clickhouse

import (
	"context"
	"encoding/hex"
	"flag"
	"fmt"
	"os"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	gormclickhouse "gorm.io/driver/clickhouse"
	"gorm.io/gorm"
)

var (
	dataSource      = flagutil.New("olap_database.data_source", "", "The clickhouse database to connect to, specified a a connection string", flagutil.SecretTag)
	maxOpenConns    = flag.Int("olap_database.max_open_conns", 0, "The maximum number of open connections to maintain to the db")
	maxIdleConns    = flag.Int("olap_database.max_idle_conns", 0, "The maximum number of idle connections to maintain to the db")
	connMaxLifetime = flag.Duration("olap_database.conn_max_lifetime", 0, "The maximum lifetime of a connection to clickhouse")

	autoMigrateDB        = flag.Bool("olap_database.auto_migrate_db", true, "If true, attempt to automigrate the db when connecting")
	autoMigrateDBAndExit = flag.Bool("olap_database.auto_migrate_db_and_exit", false, "If true, attempt to automigrate the db when connecting, then exit the program.")

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

type Table interface {
	TableName() string
	TableOptions() string
	// Fields that are in the primary DB Table schema; but not in the clickhouse schema.
	ExcludedFields() []string
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

func (i *Invocation) TableName() string {
	return "Invocations"
}

func (i *Invocation) TableOptions() string {
	engine := ""
	if *dataReplicationEnabled {
		engine = fmt.Sprintf("ReplicatedReplacingMergeTree('%s', '%s')", *zooPath, *replicaName)
	} else {
		engine = "ReplacingMergeTree()"
	}
	// Note: the sorting key need to be able to uniquely identify the invocation.
	// ReplacingMergeTree will remove entries with the same sorting key in the background.
	return fmt.Sprintf("ENGINE=%s ORDER BY (group_id, updated_at_usec, invocation_uuid)", engine)
}

func (i *Invocation) TableClusterOption() string {
	if *dataReplicationEnabled {
		return fmt.Sprintf("on cluster '%s'", *clusterName)
	}
	return ""
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

func (h *DBHandle) FlushInvocationStats(ctx context.Context, ti *tables.Invocation) error {
	inv := ToInvocationFromPrimaryDB(ti)
	res := h.DB(ctx).Create(inv)
	return res.Error
}

func runMigrations(gdb *gorm.DB) error {
	log.Info("Auto-migrating clickhouse DB")
	gdb = gdb.Set("gorm:table_options", (&Invocation{}).TableOptions())
	if clusterOpts := (&Invocation{}).TableClusterOption(); clusterOpts != "" {
		gdb = gdb.Set("gorm:table_cluster_options", clusterOpts)
	}
	return gdb.AutoMigrate(&Invocation{})
}

func Register(env environment.Env) error {
	if *dataSource == "" {
		return nil
	}
	options, err := clickhouse.ParseDSN(*dataSource)
	if err != nil {
		return status.InternalErrorf("failed to parse clickhouse data source (%q): %s", *dataSource, err)
	}
	if *maxOpenConns != 0 {
		options.MaxOpenConns = *maxOpenConns
	}
	if *maxIdleConns != 0 {
		options.MaxIdleConns = *maxIdleConns
	}
	if *connMaxLifetime != 0 {
		options.ConnMaxLifetime = *connMaxLifetime
	}

	sqlDB := clickhouse.OpenDB(options)

	db, err := gorm.Open(gormclickhouse.New(gormclickhouse.Config{
		Conn: sqlDB,
	}))
	if err != nil {
		return status.InternalErrorf("failed to open gorm clickhouse db: %s", err)
	}
	if *autoMigrateDBAndExit {
		if err := runMigrations(db); err != nil {
			log.Fatalf("Clickhouse Database auto-migration failed: %s", err)
		}
		log.Infof("Clickhouse database migration completed. Exiting due to --clickhouse.auto_migrate_db_and_exit.")
		os.Exit(0)
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

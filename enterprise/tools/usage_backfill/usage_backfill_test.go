package main_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmysql"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	gormclickhouse "gorm.io/driver/clickhouse"
)

const (
	testPeriodStart        = "2020-05-01T00:00:00Z"
	testClickHouseBoundary = "2020-06-01T00:00:00Z"
)

// Set via x_defs in the BUILD file.
var usageBackfillRunfilePath string

func toUsec(timestamp string) int64 {
	t, err := time.Parse(time.RFC3339, timestamp)
	if err != nil {
		return 0
	}
	return t.UnixMicro()
}

func toTime(timestamp string) time.Time {
	return time.UnixMicro(toUsec(timestamp)).UTC()
}

func labels(m map[string]string) *orderedmap.Map[sku.LabelName, sku.LabelValue] {
	return orderedmap.FromMap(m)
}

func TestUsageBackfillBinary_FromMySQLRows(t *testing.T) {
	ctx := context.Background()
	mysqlDataSource := testmysql.Start(t, true /*=reuseServer*/)
	flags.Set(t, "database.data_source", mysqlDataSource)

	healthChecker := healthcheck.NewHealthChecker("usage_backfill_test")
	t.Cleanup(func() {
		healthChecker.Shutdown()
		healthChecker.WaitForGracefulShutdown()
	})
	env := real_environment.NewRealEnv(healthChecker)
	dbh, err := db.GetConfiguredDatabase(ctx, env)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dbh.Close())
	})
	mysqlGORMDB := dbh.GORM(ctx, "usage_backfill_test_mysql")

	mysqlSQLDB, err := sql.Open("mysql", strings.TrimPrefix(mysqlDataSource, "mysql://"))
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, mysqlSQLDB.Close())
	})
	require.NoError(t, mysqlSQLDB.Ping())

	clickhouseDataSource := testclickhouse.Start(t, true /*=reuseServer*/)
	clickhouseOptions, err := ch.ParseDSN(clickhouseDataSource)
	require.NoError(t, err)
	clickhouseSQLDB := ch.OpenDB(clickhouseOptions)
	t.Cleanup(func() {
		require.NoError(t, clickhouseSQLDB.Close())
	})
	clickhouseGORMDB, err := gorm.Open(gormclickhouse.New(gormclickhouse.Config{
		Conn:                         clickhouseSQLDB,
		DontSupportEmptyDefaultValue: true,
	}))
	require.NoError(t, err)
	require.NoError(t, schema.RunMigrations(clickhouseGORMDB))

	clickhouseConn, err := ch.Open(clickhouseOptions)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, clickhouseConn.Close())
	})
	require.NoError(t, clickhouseConn.Ping(ctx))

	now := time.Now().UTC()
	currentMonthPeriodStart := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), 0, 0, time.UTC)
	futureMonthPeriodStart := time.Date(now.Year(), now.Month()+2, 1, 0, 0, 0, 0, time.UTC)

	for _, testCase := range []struct {
		name                   string
		rawUsageInitiallyEmpty bool
		mysqlRows              []tables.Usage
		expectedRawUsage       []schema.RawUsage
	}{
		{
			name: "exhaustive usage aggregate",
			mysqlRows: []tables.Usage{{
				GroupID:         "GR1",
				PeriodStartUsec: toUsec(testPeriodStart),
				Region:          "us-west1",
				UsageLabels: tables.UsageLabels{
					Origin: sku.OriginInternal,
					Client: sku.ClientBazel,
					Server: sku.ServerApp,
				},
				UsageCounts: tables.UsageCounts{
					Invocations:                          1,
					CASCacheHits:                         2,
					ActionCacheHits:                      3,
					TotalDownloadSizeBytes:               4,
					LinuxExecutionDurationUsec:           5,
					MacExecutionDurationUsec:             6,
					SelfHostedLinuxExecutionDurationUsec: 7,
					SelfHostedMacExecutionDurationUsec:   8,
					TotalUploadSizeBytes:                 9,
					TotalCachedActionExecUsec:            10,
					CPUNanos:                             11,
					MemoryGBUsec:                         12,
				},
			}},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.BuildEventsBESCount,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       1,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteCacheCASHits,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       2,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteCacheACHits,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       3,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteCacheCASDownloadedBytes,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       4,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteCacheCASUploadedBytes,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       9,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteCacheACCachedExecDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       10_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       5_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.OS: sku.OSMac, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       6_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedTrue, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       7_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.OS: sku.OSMac, sku.SelfHosted: sku.SelfHostedTrue, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       8_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerCPUNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       11,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerMemoryGBNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       12_000,
				},
			},
		},
		{
			name: "execution duration expands into OS and self hosted labels",
			mysqlRows: []tables.Usage{{
				GroupID:         "GR1",
				PeriodStartUsec: toUsec(testPeriodStart),
				Region:          "us-west1",
				UsageLabels: tables.UsageLabels{
					Origin: sku.OriginInternal,
					Client: sku.ClientExecutor,
					Server: sku.ServerApp,
				},
				UsageCounts: tables.UsageCounts{
					LinuxExecutionDurationUsec:           60,
					MacExecutionDurationUsec:             70,
					SelfHostedLinuxExecutionDurationUsec: 80,
					SelfHostedMacExecutionDurationUsec:   90,
				},
			}},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientExecutor, sku.Origin: sku.OriginInternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       60_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientExecutor, sku.Origin: sku.OriginInternal, sku.OS: sku.OSMac, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       70_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientExecutor, sku.Origin: sku.OriginInternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedTrue, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       80_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteWorkerDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientExecutor, sku.Origin: sku.OriginInternal, sku.OS: sku.OSMac, sku.SelfHosted: sku.SelfHostedTrue, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       90_000,
				},
			},
		},
		{
			name: "cached action execution duration converts usec to nanos",
			mysqlRows: []tables.Usage{{
				GroupID:         "GR1",
				PeriodStartUsec: toUsec(testPeriodStart),
				Region:          "us-west1",
				UsageLabels: tables.UsageLabels{
					Origin: sku.OriginInternal,
					Client: sku.ClientBazel,
					Server: sku.ServerCacheProxy,
				},
				UsageCounts: tables.UsageCounts{
					TotalCachedActionExecUsec: 42,
				},
			}},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteCacheACCachedExecDurationNanos,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerCacheProxy}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       42_000,
				},
			},
		},
		{
			name: "compute duration columns become fixed and flexible compute SKUs",
			mysqlRows: []tables.Usage{{
				GroupID:         "GR1",
				PeriodStartUsec: toUsec(testPeriodStart),
				Region:          "us-west1",
				UsageLabels: tables.UsageLabels{
					Origin: sku.OriginExternal,
					Client: sku.ClientBazel,
					Server: sku.ServerApp,
				},
				UsageCounts: tables.UsageCounts{
					LinuxArm64ExecutionBurstableComputeDurationUsec:   101,
					LinuxArm64ExecutionComputeDurationUsec:            103,
					LinuxX86_64ExecutionBurstableComputeDurationUsec:  107,
					LinuxX86_64ExecutionComputeDurationUsec:           109,
					DarwinArm64ExecutionBurstableComputeDurationUsec:  113,
					DarwinArm64ExecutionComputeDurationUsec:           127,
					DarwinX86_64ExecutionBurstableComputeDurationUsec: 131,
					DarwinX86_64ExecutionComputeDurationUsec:          137,
				},
			}},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteFlexibleComputeNanos,
					Labels:      labels(map[string]string{sku.Arch: sku.ArchArm64, sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       101_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
					Labels:      labels(map[string]string{sku.Arch: sku.ArchArm64, sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       103_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteFlexibleComputeNanos,
					Labels:      labels(map[string]string{sku.Arch: sku.ArchX86_64, sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       107_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
					Labels:      labels(map[string]string{sku.Arch: sku.ArchX86_64, sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal, sku.OS: sku.OSLinux, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       109_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteFlexibleComputeNanos,
					Labels:      labels(map[string]string{sku.Arch: sku.ArchArm64, sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal, sku.OS: sku.OSMac, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       113_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
					Labels:      labels(map[string]string{sku.Arch: sku.ArchArm64, sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal, sku.OS: sku.OSMac, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       127_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteFlexibleComputeNanos,
					Labels:      labels(map[string]string{sku.Arch: sku.ArchX86_64, sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal, sku.OS: sku.OSMac, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       131_000,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
					Labels:      labels(map[string]string{sku.Arch: sku.ArchX86_64, sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal, sku.OS: sku.OSMac, sku.SelfHosted: sku.SelfHostedFalse, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       137_000,
				},
			},
		},
		{
			name: "legacy hourly period start is preserved",
			mysqlRows: []tables.Usage{{
				GroupID:         "GR1",
				PeriodStartUsec: toUsec("2020-05-01T13:00:00Z"),
				Region:          "us-west1",
				UsageLabels: tables.UsageLabels{
					Origin: sku.OriginInternal,
					Client: sku.ClientBazel,
					Server: sku.ServerApp,
				},
				UsageCounts: tables.UsageCounts{
					Invocations: 13,
				},
			}},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.BuildEventsBESCount,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime("2020-05-01T13:00:00Z"),
					BufferID:    "us-west1:redis",
					Count:       13,
				},
			},
		},
		{
			name:                   "empty clickhouse starts with current partial month",
			rawUsageInitiallyEmpty: true,
			mysqlRows: []tables.Usage{
				{
					GroupID:         "GR1",
					PeriodStartUsec: currentMonthPeriodStart.UnixMicro(),
					Region:          "us-west1",
					UsageLabels: tables.UsageLabels{
						Origin: sku.OriginInternal,
						Client: sku.ClientBazel,
						Server: sku.ServerApp,
					},
					UsageCounts: tables.UsageCounts{
						Invocations: 19,
					},
				},
				{
					GroupID:         "GR1",
					PeriodStartUsec: futureMonthPeriodStart.UnixMicro(),
					Region:          "us-west1",
					UsageLabels: tables.UsageLabels{
						Origin: sku.OriginInternal,
						Client: sku.ClientBazel,
						Server: sku.ServerApp,
					},
					UsageCounts: tables.UsageCounts{
						Invocations: 23,
					},
				},
			},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.BuildEventsBESCount,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: currentMonthPeriodStart,
					BufferID:    "us-west1:redis",
					Count:       19,
				},
			},
		},
		{
			name: "earliest clickhouse month is skipped",
			mysqlRows: []tables.Usage{{
				GroupID:         "GR1",
				PeriodStartUsec: toUsec(testClickHouseBoundary),
				Region:          "us-west1",
				UsageLabels: tables.UsageLabels{
					Origin: sku.OriginInternal,
					Client: sku.ClientBazel,
					Server: sku.ServerApp,
				},
				UsageCounts: tables.UsageCounts{
					Invocations: 17,
				},
			}},
			expectedRawUsage: nil,
		},
		{
			name: "month before earliest clickhouse month is included",
			mysqlRows: []tables.Usage{{
				GroupID:         "GR1",
				PeriodStartUsec: toUsec("2020-05-31T23:00:00Z"),
				Region:          "us-west1",
				UsageLabels: tables.UsageLabels{
					Origin: sku.OriginInternal,
					Client: sku.ClientBazel,
					Server: sku.ServerApp,
				},
				UsageCounts: tables.UsageCounts{
					Invocations: 31,
				},
			}},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.BuildEventsBESCount,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime("2020-05-31T23:00:00Z"),
					BufferID:    "us-west1:redis",
					Count:       31,
				},
			},
		},
		{
			name: "regions become production buffer IDs",
			mysqlRows: []tables.Usage{
				{
					GroupID:         "GR1",
					PeriodStartUsec: toUsec(testPeriodStart),
					Region:          "us-west1",
					UsageLabels: tables.UsageLabels{
						Origin: sku.OriginInternal,
						Client: sku.ClientBazel,
						Server: sku.ServerApp,
					},
					UsageCounts: tables.UsageCounts{
						Invocations: 3,
					},
				},
				{
					GroupID:         "GR1",
					PeriodStartUsec: toUsec(testPeriodStart),
					Region:          "europe-west4",
					UsageLabels: tables.UsageLabels{
						Origin: sku.OriginInternal,
						Client: sku.ClientBazel,
						Server: sku.ServerApp,
					},
					UsageCounts: tables.UsageCounts{
						Invocations: 5,
					},
				},
			},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.BuildEventsBESCount,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "europe-west4:redis",
					Count:       5,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.BuildEventsBESCount,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       3,
				},
			},
		},
		{
			name: "duplicate mysql rows are summed before writing raw usage",
			mysqlRows: []tables.Usage{
				{
					GroupID:         "GR1",
					PeriodStartUsec: toUsec(testPeriodStart),
					Region:          "us-west1",
					UsageLabels: tables.UsageLabels{
						Origin: sku.OriginInternal,
						Client: sku.ClientBazel,
						Server: sku.ServerApp,
					},
					UsageCounts: tables.UsageCounts{
						Invocations:  2,
						CASCacheHits: 4,
					},
				},
				{
					GroupID:         "GR1",
					PeriodStartUsec: toUsec(testPeriodStart),
					Region:          "us-west1",
					UsageLabels: tables.UsageLabels{
						Origin: sku.OriginInternal,
						Client: sku.ClientBazel,
						Server: sku.ServerApp,
					},
					UsageCounts: tables.UsageCounts{
						Invocations:  3,
						CASCacheHits: 5,
					},
				},
			},
			expectedRawUsage: []schema.RawUsage{
				{
					GroupID:     "GR1",
					SKU:         sku.BuildEventsBESCount,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       5,
				},
				{
					GroupID:     "GR1",
					SKU:         sku.RemoteCacheCASHits,
					Labels:      labels(map[string]string{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginInternal, sku.Server: sku.ServerApp}),
					PeriodStart: toTime(testPeriodStart),
					BufferID:    "us-west1:redis",
					Count:       9,
				},
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			_, err := mysqlSQLDB.Exec("TRUNCATE TABLE Usages")
			require.NoError(t, err)
			err = clickhouseConn.Exec(ctx, "TRUNCATE TABLE RawUsage")
			require.NoError(t, err)

			if !testCase.rawUsageInitiallyEmpty {
				batch, err := clickhouseConn.PrepareBatch(ctx, "INSERT INTO RawUsage (group_id, sku, labels, period_start, buffer_id, count)")
				require.NoError(t, err)
				err = batch.Append(
					"GR_BOUNDARY",
					sku.BuildEventsBESCount.String(),
					labels(map[string]string{sku.Client: sku.ClientBazel}),
					toTime(testClickHouseBoundary),
					"us-west1:redis",
					int64(1),
				)
				require.NoError(t, err)
				err = batch.Send()
				require.NoError(t, err)
			}

			// Seed MySQL with rows like the legacy aggregate rows that the
			// backfill tool actually sees.
			if len(testCase.mysqlRows) > 0 {
				err = mysqlGORMDB.Create(&testCase.mysqlRows).Error
				require.NoError(t, err)
			}

			// Run the actual compiled tool binary. This covers flag parsing,
			// DB connection setup, MySQL querying, conversion, and ClickHouse
			// insertion together.
			binaryPath, err := runfiles.Rlocation(usageBackfillRunfilePath)
			require.NoError(t, err)
			cmd := exec.Command(binaryPath,
				"-database.data_source="+mysqlDataSource,
				"-olap_database.data_source="+clickhouseDataSource,
			)
			cmd.Stdout = os.Stderr
			cmd.Stderr = os.Stderr
			err = cmd.Run()
			require.NoError(t, err, "run usage_backfill")

			// Query RawUsage because buffer IDs are the storage-level detail
			// that tells us each MySQL region became its production buffer ID.
			rows, err := clickhouseConn.Query(ctx, `
				SELECT
					group_id,
					sku,
					labels,
					period_start,
					buffer_id,
					count
				FROM RawUsage FINAL
				WHERE group_id != 'GR_BOUNDARY'
				ORDER BY period_start ASC, group_id ASC, sku ASC, toString(labels) ASC, buffer_id ASC, count DESC
			`)
			require.NoError(t, err)
			defer rows.Close()

			var rawUsageRows []schema.RawUsage
			for rows.Next() {
				var row schema.RawUsage
				var usageSKU string
				var labelMap map[string]string
				err = rows.Scan(&row.GroupID, &usageSKU, &labelMap, &row.PeriodStart, &row.BufferID, &row.Count)
				require.NoError(t, err)
				row.SKU = sku.SKU(usageSKU)
				row.Labels = labels(labelMap)
				row.PeriodStart = row.PeriodStart.UTC()
				rawUsageRows = append(rawUsageRows, row)
			}
			err = rows.Err()
			require.NoError(t, err)

			got := sortedRawUsageRows(rawUsageRows)
			assert.Equal(t, sortedRawUsageRows(testCase.expectedRawUsage), got)
		})
	}
}

func labelSortKey(labels *orderedmap.Map[sku.LabelName, sku.LabelValue]) string {
	if labels == nil {
		return ""
	}
	labelMap := labels.ToMap()
	keys := make([]string, 0, len(labelMap))
	for key, value := range labelMap {
		if value == "" {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+labelMap[key])
	}
	return strings.Join(parts, ";")
}

func sortedRawUsageRows(rows []schema.RawUsage) []schema.RawUsage {
	sorted := append([]schema.RawUsage(nil), rows...)
	sort.Slice(sorted, func(i, j int) bool {
		left := []string{
			sorted[i].PeriodStart.Format(time.RFC3339Nano),
			sorted[i].GroupID,
			sorted[i].SKU.String(),
			labelSortKey(sorted[i].Labels),
			sorted[i].BufferID,
		}
		right := []string{
			sorted[j].PeriodStart.Format(time.RFC3339Nano),
			sorted[j].GroupID,
			sorted[j].SKU.String(),
			labelSortKey(sorted[j].Labels),
			sorted[j].BufferID,
		}
		for k := range left {
			if left[k] != right[k] {
				return left[k] < right[k]
			}
		}
		return sorted[i].Count < sorted[j].Count
	})
	return sorted
}

package usage_service_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	usagepb "github.com/buildbuddy-io/buildbuddy/proto/usage"
)

func TestGetUsage_ReadsFromOLAPDB(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "app.read_usage_from_olap_db", true)

	group := &tables.Group{
		GroupID: "GR1",
		Model: tables.Model{
			CreatedAtUsec: time.Date(2023, 7, 9, 0, 0, 0, 0, time.UTC).UnixMicro(),
		},
	}
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	now := time.Date(2024, 2, 22, 12, 0, 0, 0, time.UTC)
	service, err := usage_service.New(env, clockwork.NewFakeClockAt(now))
	require.NoError(t, err)

	// Seed primary DB usage that would be visible if the OLAP flag were ignored.
	err = env.GetDBHandle().NewQuery(ctx, "test_create_primary_db_usage").Create(&tables.Usage{
		UsageID:         "UG1",
		GroupID:         "GR1",
		PeriodStartUsec: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
		UsageCounts:     tables.UsageCounts{Invocations: 999, CASCacheHits: 999},
	})
	require.NoError(t, err)

	// Seed raw OLAP usage across multiple minute buckets. The Usage view
	// aggregates these rows by SKU and labels, and the Usage page query then
	// groups them into daily usage protos.
	require.NoError(t, env.GetOLAPDBHandle().FlushUsages(ctx, []*schema.RawUsage{
		{
			GroupID:     "GR1",
			SKU:         sku.BuildEventsBESCount,
			Labels:      orderedmap.FromMap(map[sku.LabelName]sku.LabelValue(nil)),
			PeriodStart: time.Date(2024, 2, 3, 0, 1, 0, 0, time.UTC),
			Count:       13,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteCacheCASHits,
			Labels:      orderedmap.FromMap(map[sku.LabelName]sku.LabelValue(nil)),
			PeriodStart: time.Date(2024, 2, 3, 0, 2, 0, 0, time.UTC),
			Count:       10_000,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteCacheACCachedExecDurationNanos,
			Labels:      orderedmap.FromMap(map[sku.LabelName]sku.LabelValue(nil)),
			PeriodStart: time.Date(2024, 2, 3, 0, 3, 0, 0, time.UTC),
			Count:       9_000,
		},
		// External origin rows should be included in total and external
		// download counts, but excluded from internal and workflow counts.
		{
			GroupID: "GR1",
			SKU:     sku.RemoteCacheCASDownloadedBytes,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin: sku.OriginExternal,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 4, 0, 0, time.UTC),
			Count:       101,
		},
		// Internal cache usage from executors should be counted as cloud RBE
		// usage, not workflow usage.
		{
			GroupID: "GR1",
			SKU:     sku.RemoteCacheCASDownloadedBytes,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin: sku.OriginInternal,
				sku.Client: sku.ClientExecutor,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 5, 0, 0, time.UTC),
			Count:       202,
		},
		// Internal cache usage from bazel should be counted as workflow usage,
		// since BuildBuddy workflows use bazel as a client.
		{
			GroupID: "GR1",
			SKU:     sku.RemoteCacheCASDownloadedBytes,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin: sku.OriginInternal,
				sku.Client: sku.ClientBazel,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 6, 0, 0, time.UTC),
			Count:       303,
		},
		// External origin rows should be included in total and external upload
		// counts, but excluded from internal and workflow upload counts.
		{
			GroupID: "GR1",
			SKU:     sku.RemoteCacheCASUploadedBytes,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin: sku.OriginExternal,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 6, 30, 0, time.UTC),
			Count:       404,
		},
		{
			GroupID: "GR1",
			SKU:     sku.RemoteCacheCASUploadedBytes,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin: sku.OriginInternal,
				sku.Client: sku.ClientExecutor,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 6, 40, 0, time.UTC),
			Count:       505,
		},
		{
			GroupID: "GR1",
			SKU:     sku.RemoteCacheCASUploadedBytes,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin: sku.OriginInternal,
				sku.Client: sku.ClientBazel,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 6, 50, 0, time.UTC),
			Count:       606,
		},
		{
			GroupID: "GR1",
			SKU:     sku.RemoteExecutionExecuteWorkerDurationNanos,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin:     sku.OriginInternal,
				sku.Client:     sku.ClientExecutor,
				sku.OS:         sku.OSLinux,
				sku.SelfHosted: sku.SelfHostedFalse,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 7, 0, 0, time.UTC),
			Count:       7_000,
		},
		{
			GroupID: "GR1",
			SKU:     sku.RemoteExecutionExecuteWorkerDurationNanos,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin:     sku.OriginInternal,
				sku.Client:     sku.ClientBazel,
				sku.OS:         sku.OSLinux,
				sku.SelfHosted: sku.SelfHostedFalse,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 7, 30, 0, time.UTC),
			Count:       17_000,
		},
		{
			GroupID: "GR1",
			SKU:     sku.RemoteExecutionExecuteWorkerCPUNanos,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin:     sku.OriginInternal,
				sku.Client:     sku.ClientExecutor,
				sku.OS:         sku.OSLinux,
				sku.SelfHosted: sku.SelfHostedFalse,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 8, 0, 0, time.UTC),
			Count:       11_000,
		},
		// Workflow CPU should be counted in total cloud CPU and workflow CPU,
		// but excluded from cloud RBE CPU.
		{
			GroupID: "GR1",
			SKU:     sku.RemoteExecutionExecuteWorkerCPUNanos,
			Labels: orderedmap.FromMap(map[sku.LabelName]sku.LabelValue{
				sku.Origin:     sku.OriginInternal,
				sku.Client:     sku.ClientBazel,
				sku.OS:         sku.OSLinux,
				sku.SelfHosted: sku.SelfHostedFalse,
			}),
			PeriodStart: time.Date(2024, 2, 3, 0, 9, 0, 0, time.UTC),
			Count:       13_000,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.BuildEventsBESCount,
			Labels:      orderedmap.FromMap(map[sku.LabelName]sku.LabelValue(nil)),
			PeriodStart: time.Date(2024, 2, 4, 0, 1, 0, 0, time.UTC),
			Count:       15,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteCacheCASHits,
			Labels:      orderedmap.FromMap(map[sku.LabelName]sku.LabelValue(nil)),
			PeriodStart: time.Date(2024, 2, 4, 0, 2, 0, 0, time.UTC),
			Count:       12_000,
		},
		// Usage outside the requested month should not be returned.
		{
			GroupID:     "GR1",
			SKU:         sku.BuildEventsBESCount,
			Labels:      orderedmap.FromMap(map[sku.LabelName]sku.LabelValue(nil)),
			PeriodStart: time.Date(2024, 1, 3, 0, 1, 0, 0, time.UTC),
			Count:       77,
		},
		// Usage for a different group should not be returned.
		{
			GroupID:     "GR2",
			SKU:         sku.BuildEventsBESCount,
			Labels:      orderedmap.FromMap(map[sku.LabelName]sku.LabelValue(nil)),
			PeriodStart: time.Date(2024, 2, 3, 0, 1, 0, 0, time.UTC),
			Count:       107,
		},
	}))

	rsp, err := service.GetUsageInternal(ctx, group, &usagepb.GetUsageRequest{
		RequestContext: &ctxpb.RequestContext{GroupId: "GR1"},
		UsagePeriod:    "2024-02",
	})
	require.NoError(t, err)

	expectedResponse := &usagepb.GetUsageResponse{
		Usage: &usagepb.Usage{
			Period:                                  "2024-02",
			Invocations:                             28,
			CasCacheHits:                            22_000,
			TotalCachedActionExecUsec:               9,
			TotalDownloadSizeBytes:                  606,
			TotalExternalDownloadSizeBytes:          101,
			TotalInternalDownloadSizeBytes:          202,
			TotalWorkflowDownloadSizeBytes:          303,
			TotalUploadSizeBytes:                    1515,
			TotalExternalUploadSizeBytes:            404,
			TotalInternalUploadSizeBytes:            505,
			TotalWorkflowUploadSizeBytes:            606,
			LinuxExecutionDurationUsec:              24,
			CloudRbeLinuxExecutionDurationUsec:      7,
			CloudWorkflowLinuxExecutionDurationUsec: 17,
			CloudCpuNanos:                           24_000,
			CloudRbeCpuNanos:                        11_000,
			CloudWorkflowCpuNanos:                   13_000,
		},
		DailyUsage: []*usagepb.Usage{
			{
				Period:                                  "2024-02-03",
				Invocations:                             13,
				CasCacheHits:                            10_000,
				TotalCachedActionExecUsec:               9,
				TotalDownloadSizeBytes:                  606,
				TotalExternalDownloadSizeBytes:          101,
				TotalInternalDownloadSizeBytes:          202,
				TotalWorkflowDownloadSizeBytes:          303,
				TotalUploadSizeBytes:                    1515,
				TotalExternalUploadSizeBytes:            404,
				TotalInternalUploadSizeBytes:            505,
				TotalWorkflowUploadSizeBytes:            606,
				LinuxExecutionDurationUsec:              24,
				CloudRbeLinuxExecutionDurationUsec:      7,
				CloudWorkflowLinuxExecutionDurationUsec: 17,
				CloudCpuNanos:                           24_000,
				CloudRbeCpuNanos:                        11_000,
				CloudWorkflowCpuNanos:                   13_000,
			},
			{
				Period:       "2024-02-04",
				Invocations:  15,
				CasCacheHits: 12_000,
			},
		},
		AvailableUsagePeriods: []string{
			"2024-02",
			"2024-01",
			"2023-12",
			"2023-11",
			"2023-10",
			"2023-09",
			"2023-08",
			"2023-07",
		},
	}
	assert.Empty(t, cmp.Diff(expectedResponse, rsp, protocmp.Transform()))
}

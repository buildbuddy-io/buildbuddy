package usage_service_test

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
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
		GroupID:       "GR1",
		CreatedAtUsec: time.Date(2023, 7, 9, 0, 0, 0, 0, time.UTC).UnixMicro(),
	}
	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	now := time.Date(2024, 2, 22, 12, 0, 0, 0, time.UTC)
	service, err := usage_service.New(env, clockwork.NewFakeClockAt(now))
	require.NoError(t, err)

	rbeFirecrackerLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutor,
		sku.Origin:        sku.OriginInternal,
		sku.Server:        sku.ServerApp,
		sku.OS:            sku.OSLinux,
		sku.Arch:          sku.ArchX86_64,
		sku.SelfHosted:    sku.SelfHostedFalse,
		sku.IsolationType: "firecracker",
	}
	workflowFirecrackerLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutorWorkflows,
		sku.Origin:        sku.OriginInternal,
		sku.Server:        sku.ServerApp,
		sku.OS:            sku.OSLinux,
		sku.Arch:          sku.ArchX86_64,
		sku.SelfHosted:    sku.SelfHostedFalse,
		sku.IsolationType: "firecracker",
	}
	rbeOCILabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutor,
		sku.Origin:        sku.OriginInternal,
		sku.Server:        sku.ServerApp,
		sku.OS:            sku.OSLinux,
		sku.Arch:          sku.ArchX86_64,
		sku.SelfHosted:    sku.SelfHostedFalse,
		sku.IsolationType: "oci",
	}
	// Same execution dimensions as rbeFirecrackerLabels, differing only by a
	// label that the Usage page doesn't return.
	rbeFirecrackerNoOriginLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutor,
		sku.Server:        sku.ServerApp,
		sku.OS:            sku.OSLinux,
		sku.Arch:          sku.ArchX86_64,
		sku.SelfHosted:    sku.SelfHostedFalse,
		sku.IsolationType: "firecracker",
	}
	selfHostedFirecrackerLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutor,
		sku.Server:        sku.ServerApp,
		sku.OS:            sku.OSLinux,
		sku.Arch:          sku.ArchX86_64,
		sku.SelfHosted:    sku.SelfHostedTrue,
		sku.IsolationType: "firecracker",
	}

	// Seed primary DB usage that would be visible if the OLAP flag were ignored.
	err = env.GetDBHandle().NewQuery(ctx, "test_create_primary_db_usage").Create(&tables.Usage{
		UsageID:         "UG1",
		GroupID:         "GR1",
		PeriodStartUsec: time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC).UnixMicro(),
		Invocations:     999,
		CASCacheHits:    999,
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
		// Compute usage is returned per label combination, summed across the
		// whole month.
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
			Labels:      orderedmap.FromMap(rbeFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 10, 0, 0, time.UTC),
			Count:       5_000,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
			Labels:      orderedmap.FromMap(rbeFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 4, 0, 10, 0, 0, time.UTC),
			Count:       6_000,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
			Labels:      orderedmap.FromMap(workflowFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 11, 0, 0, time.UTC),
			Count:       7_000,
		},
		// Rows that differ only by labels the Usage page doesn't return are
		// merged into the row with the same execution dimensions.
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
			Labels:      orderedmap.FromMap(rbeFirecrackerNoOriginLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 10, 0, 0, time.UTC),
			Count:       1_000,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
			Labels:      orderedmap.FromMap(selfHostedFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 11, 0, 0, time.UTC),
			Count:       9_000,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFlexibleComputeNanos,
			Labels:      orderedmap.FromMap(rbeOCILabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 12, 0, 0, time.UTC),
			Count:       8_000,
		},
		// Label combinations with zero usage should not be returned.
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFlexibleComputeNanos,
			Labels:      orderedmap.FromMap(workflowFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 12, 0, 0, time.UTC),
			Count:       0,
		},
		// Compute nanoseconds that overflow Int64 when summed must still be
		// returned correctly, as microseconds.
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFlexibleComputeNanos,
			Labels:      orderedmap.FromMap(selfHostedFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 12, 0, 0, time.UTC),
			Count:       math.MaxInt64,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFlexibleComputeNanos,
			Labels:      orderedmap.FromMap(selfHostedFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 13, 0, 0, time.UTC),
			Count:       1_000,
		},
		// Snapshot bytes are also returned per label combination.
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteRemoteSnapshotSavedBytes,
			Labels:      orderedmap.FromMap(workflowFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 13, 0, 0, time.UTC),
			Count:       1_001,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteRemoteSnapshotSavedBytes,
			Labels:      orderedmap.FromMap(rbeFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 13, 0, 0, time.UTC),
			Count:       2_002,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteLocalSnapshotSavedBytes,
			Labels:      orderedmap.FromMap(workflowFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 14, 0, 0, time.UTC),
			Count:       3_003,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteLocalSnapshotSavedBytes,
			Labels:      orderedmap.FromMap(rbeFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 14, 0, 0, time.UTC),
			Count:       4_004,
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
		// OLAP-only usage outside the requested month should not be returned.
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
			Labels:      orderedmap.FromMap(rbeFirecrackerLabels),
			PeriodStart: time.Date(2024, 1, 3, 0, 10, 0, 0, time.UTC),
			Count:       77_000,
		},
		{
			GroupID:     "GR1",
			SKU:         sku.RemoteExecutionExecuteRemoteSnapshotSavedBytes,
			Labels:      orderedmap.FromMap(rbeFirecrackerLabels),
			PeriodStart: time.Date(2024, 1, 3, 0, 13, 0, 0, time.UTC),
			Count:       77,
		},
		// OLAP-only usage for a different group should not be returned.
		{
			GroupID:     "GR2",
			SKU:         sku.RemoteExecutionExecuteFixedComputeNanos,
			Labels:      orderedmap.FromMap(rbeFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 10, 0, 0, time.UTC),
			Count:       107_000,
		},
		{
			GroupID:     "GR2",
			SKU:         sku.RemoteExecutionExecuteRemoteSnapshotSavedBytes,
			Labels:      orderedmap.FromMap(rbeFirecrackerLabels),
			PeriodStart: time.Date(2024, 2, 3, 0, 13, 0, 0, time.UTC),
			Count:       107,
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
			// Execution usage rows are expected in the query's order: cloud before
			// self-hosted, RBE before workflows, then by isolation type, OS and
			// arch. Compute usage is converted from nanoseconds to microseconds.
			FixedComputeUsec: []*usagepb.ExecutionUsage{
				{IsolationType: "firecracker", Os: "linux", Arch: "x86_64", Count: 12},
				{Workflow: true, IsolationType: "firecracker", Os: "linux", Arch: "x86_64", Count: 7},
				{SelfHosted: true, IsolationType: "firecracker", Os: "linux", Arch: "x86_64", Count: 9},
			},
			FlexibleComputeUsec: []*usagepb.ExecutionUsage{
				{IsolationType: "oci", Os: "linux", Arch: "x86_64", Count: 8},
				{SelfHosted: true, IsolationType: "firecracker", Os: "linux", Arch: "x86_64", Count: (math.MaxInt64 + 1_000) / 1_000},
			},
			RemoteSnapshotSavedBytes: []*usagepb.ExecutionUsage{
				{IsolationType: "firecracker", Os: "linux", Arch: "x86_64", Count: 2_002},
				{Workflow: true, IsolationType: "firecracker", Os: "linux", Arch: "x86_64", Count: 1_001},
			},
			LocalSnapshotSavedBytes: []*usagepb.ExecutionUsage{
				{IsolationType: "firecracker", Os: "linux", Arch: "x86_64", Count: 4_004},
				{Workflow: true, IsolationType: "firecracker", Os: "linux", Arch: "x86_64", Count: 3_003},
			},
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

func TestUsageExport(t *testing.T) {
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "app.read_usage_from_olap_db", true)

	ctx := context.Background()
	env := testenv.GetTestEnv(t)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	env.SetAuthenticator(ta)
	service, err := usage_service.New(env, clockwork.NewFakeClockAt(time.Date(2024, 2, 22, 12, 0, 0, 0, time.UTC)))
	require.NoError(t, err)

	rbeLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutor,
		sku.Origin:        sku.OriginInternal,
		sku.Server:        sku.ServerApp,
		sku.OS:            sku.OSLinux,
		sku.Arch:          sku.ArchX86_64,
		sku.SelfHosted:    sku.SelfHostedFalse,
		sku.IsolationType: "firecracker",
	}
	workflowLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutorWorkflows,
		sku.Origin:        sku.OriginInternal,
		sku.Server:        sku.ServerApp,
		sku.OS:            sku.OSLinux,
		sku.Arch:          sku.ArchX86_64,
		sku.SelfHosted:    sku.SelfHostedFalse,
		sku.IsolationType: "firecracker",
	}
	selfHostedLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutor,
		sku.OS:            sku.OSLinux,
		sku.Arch:          sku.ArchArm64,
		sku.SelfHosted:    sku.SelfHostedTrue,
		sku.IsolationType: "oci",
	}
	macLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client:        sku.ClientExecutor,
		sku.OS:            sku.OSMac,
		sku.Arch:          sku.ArchArm64,
		sku.SelfHosted:    sku.SelfHostedFalse,
		sku.IsolationType: "none",
	}
	externalLabels := map[sku.LabelName]sku.LabelValue{sku.Origin: sku.OriginExternal}
	internalLabels := map[sku.LabelName]sku.LabelValue{sku.Origin: sku.OriginInternal, sku.Client: sku.ClientExecutor}
	internalBazelLabels := map[sku.LabelName]sku.LabelValue{sku.Origin: sku.OriginInternal, sku.Client: sku.ClientBazel}
	customerProxyLabels := map[sku.LabelName]sku.LabelValue{sku.Origin: sku.OriginExternal, sku.Proxy: sku.ProxyCustomer}

	day3 := time.Date(2024, 2, 3, 0, 0, 0, 0, time.UTC)
	day4 := time.Date(2024, 2, 4, 0, 0, 0, 0, time.UTC)
	// Each row gets its own minute so that none are deduplicated.
	n := 0
	row := func(groupID string, day time.Time, s sku.SKU, labels map[sku.LabelName]sku.LabelValue, count int64) *schema.RawUsage {
		n++
		return &schema.RawUsage{
			GroupID:     groupID,
			SKU:         s,
			Labels:      orderedmap.FromMap(labels),
			PeriodStart: day.Add(time.Duration(n) * time.Minute),
			Count:       count,
		}
	}
	require.NoError(t, env.GetOLAPDBHandle().FlushUsages(ctx, []*schema.RawUsage{
		// Usage without execution dimensions is exported on one row per day.
		row("GR1", day3, sku.BuildEventsBESCount, nil, 13),
		row("GR1", day3, sku.RemoteCacheACHits, nil, 5),
		row("GR1", day3, sku.RemoteCacheACCachedExecDurationNanos, nil, int64(90*time.Second)),
		row("GR1", day3, sku.RemoteCacheCASHits, nil, 10_000),
		row("GR1", day3, sku.RemoteCacheCASDownloadedBytes, externalLabels, 101),
		row("GR1", day3, sku.RemoteCacheCASDownloadedBytes, internalLabels, 202),
		row("GR1", day3, sku.RemoteCacheCASDownloadedBytes, internalBazelLabels, 303),
		// Customer proxy bytes are not counted as external bytes.
		row("GR1", day3, sku.RemoteCacheCASDownloadedBytes, customerProxyLabels, 700),
		row("GR1", day3, sku.RemoteCacheCASUploadedBytes, externalLabels, 404),
		row("GR1", day3, sku.RemoteCacheCASUploadedBytes, internalLabels, 505),
		row("GR1", day3, sku.RemoteCacheCASUploadedBytes, internalBazelLabels, 606),
		row("GR1", day3, sku.RemoteCacheCASUploadedBytes, customerProxyLabels, 800),
		// Execution usage is exported per combination of execution dimensions,
		// in minutes.
		row("GR1", day3, sku.RemoteExecutionExecuteWorkerDurationNanos, rbeLabels, int64(2*time.Minute)),
		row("GR1", day3, sku.RemoteExecutionExecuteWorkerDurationNanos, rbeLabels, int64(time.Minute)),
		row("GR1", day3, sku.RemoteExecutionExecuteWorkerDurationNanos, workflowLabels, int64(3*time.Minute)),
		row("GR1", day3, sku.RemoteExecutionExecuteWorkerDurationNanos, selfHostedLabels, int64(30*time.Second)),
		row("GR1", day3, sku.RemoteExecutionExecuteWorkerCPUNanos, rbeLabels, int64(time.Minute)),
		row("GR1", day3, sku.RemoteExecutionExecuteWorkerCPUNanos, selfHostedLabels, int64(90*time.Second)),
		row("GR1", day3, sku.RemoteExecutionExecuteFixedComputeNanos, rbeLabels, int64(30*time.Second)),
		row("GR1", day3, sku.RemoteExecutionExecuteFixedComputeNanos, workflowLabels, int64(6*time.Second)),
		row("GR1", day3, sku.RemoteExecutionExecuteFlexibleComputeNanos, selfHostedLabels, int64(6*time.Second)),
		row("GR1", day3, sku.RemoteExecutionExecuteRemoteSnapshotSavedBytes, workflowLabels, 1_001),
		row("GR1", day3, sku.RemoteExecutionExecuteLocalSnapshotSavedBytes, rbeLabels, 4_004),
		// Dimensions with zero usage are not exported.
		row("GR1", day3, sku.RemoteExecutionExecuteFlexibleComputeNanos, macLabels, 0),
		row("GR1", day4, sku.BuildEventsBESCount, nil, 15),
		row("GR1", day4, sku.RemoteExecutionExecuteFixedComputeNanos, rbeLabels, int64(90*time.Second)),
		row("GR1", day4, sku.RemoteExecutionExecuteRemoteSnapshotSavedBytes, rbeLabels, 2_002),
		// Usage outside the date range or for another group is not exported.
		row("GR1", day3.AddDate(0, -1, 0), sku.BuildEventsBESCount, nil, 77),
		row("GR2", day3, sku.BuildEventsBESCount, nil, 107),
	}))

	authCtx, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)
	export := func(ctx context.Context, url string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		service.GetUsageExportHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, url, nil).WithContext(ctx))
		return rec
	}

	rec := export(authCtx, "/usage/download?start=2024-02-01&end=2024-03-01")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "text/csv; charset=utf-8", rec.Header().Get("Content-Type"))
	assert.Equal(t, `attachment; filename="usage-2024-02-01-2024-02-29.csv"`, rec.Header().Get("Content-Disposition"))
	assert.Equal(t, strings.Join([]string{
		"time,invocations,action_cache_hits,cached_build_minutes,cas_cache_hits,external_download_bytes,internal_download_bytes,workflow_download_bytes,customer_proxy_download_bytes,external_upload_bytes,internal_upload_bytes,workflow_upload_bytes,customer_proxy_upload_bytes,is_workflow,is_self_hosted,arch,os,isolation_type,execution_minutes,cpu_minutes,fixed_compute_unit_minutes,flexible_compute_unit_minutes,remote_snapshot_saved_bytes,local_snapshot_saved_bytes",
		"2024-02-03,13,5,1.500,10000,101,202,303,700,404,505,606,800,,,,,,0.000,0.000,0.000,0.000,0,0",
		"2024-02-03,0,0,0.000,0,0,0,0,0,0,0,0,0,false,false,x86_64,linux,firecracker,3.000,1.000,0.500,0.000,0,4004",
		"2024-02-03,0,0,0.000,0,0,0,0,0,0,0,0,0,false,true,arm64,linux,oci,0.500,1.500,0.000,0.100,0,0",
		"2024-02-03,0,0,0.000,0,0,0,0,0,0,0,0,0,true,false,x86_64,linux,firecracker,3.000,0.000,0.100,0.000,1001,0",
		"2024-02-04,15,0,0.000,0,0,0,0,0,0,0,0,0,,,,,,0.000,0.000,0.000,0.000,0,0",
		"2024-02-04,0,0,0.000,0,0,0,0,0,0,0,0,0,false,false,x86_64,linux,firecracker,0.000,0.000,1.500,0.000,2002,0",
		"",
	}, "\n"), rec.Body.String())

	for _, url := range []string{
		"/usage/download",
		"/usage/download?start=2024-02-30&end=2024-03-01",
		"/usage/download?start=2024-03-01&end=2024-02-01",
		"/usage/download?start=2023-01-01&end=2024-06-01",
	} {
		assert.Equal(t, http.StatusBadRequest, export(authCtx, url).Code, url)
	}
	assert.Equal(t, http.StatusUnauthorized, export(ctx, "/usage/download?start=2024-02-01&end=2024-03-01").Code)
}

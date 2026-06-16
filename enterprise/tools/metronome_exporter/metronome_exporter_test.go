package main

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

type fakeMetronomeClient struct {
	reported [][]metronome.UsageEvent
}

func (c *fakeMetronomeClient) ReportUsage(ctx context.Context, events []metronome.UsageEvent) error {
	c.reported = append(c.reported, append([]metronome.UsageEvent(nil), events...))
	return nil
}

func TestExportAll_ReportsUsageForMultipleWindows(t *testing.T) {
	env := setupClickHouseEnv(t)
	ctx := t.Context()

	from := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	to := from.Add(3 * metronome.WindowSize)
	bazelLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client: sku.ClientBazel,
		sku.Origin: sku.OriginExternal,
	}
	internalLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client: sku.ClientBazel,
		sku.Origin: sku.OriginInternal,
	}

	// Write usage data to clickhouse.
	rows := []*schema.RawUsage{
		rawUsage("GR1", from, sku.BuildEventsBESCount, bazelLabels, 2),
		rawUsage("GR1", from.Add(metronome.WindowSize), sku.BuildEventsBESCount, bazelLabels, 3),
		rawUsage("GR2", from.Add(metronome.WindowSize), sku.RemoteCacheCASHits, internalLabels, 7),
		rawUsage("GR2", from.Add(2*metronome.WindowSize), sku.RemoteCacheCASHits, internalLabels, 11),
		// The to-timestamp is exclusive, so this event should not be reported.
		rawUsage("GR3", to, sku.BuildEventsBESCount, bazelLabels, 100),
	}
	require.NoError(t, env.GetOLAPDBHandle().FlushUsages(ctx, rows))

	client := &fakeMetronomeClient{}
	require.NoError(t, exportAll(ctx, env, client, nil, &window{from: from, to: to}))

	require.Equal(t, [][]metronome.UsageEvent{
		{
			{
				GroupID:     "GR1",
				PeriodStart: from,
				PeriodEnd:   from.Add(metronome.WindowSize),
				SKU:         sku.BuildEventsBESCount,
				Labels:      bazelLabels,
				Count:       2,
			},
		},
		{
			{
				GroupID:     "GR1",
				PeriodStart: from.Add(metronome.WindowSize),
				PeriodEnd:   from.Add(2 * metronome.WindowSize),
				SKU:         sku.BuildEventsBESCount,
				Labels:      bazelLabels,
				Count:       3,
			},
			{
				GroupID:     "GR2",
				PeriodStart: from.Add(metronome.WindowSize),
				PeriodEnd:   from.Add(2 * metronome.WindowSize),
				SKU:         sku.RemoteCacheCASHits,
				Labels:      internalLabels,
				Count:       7,
			},
		},
		{
			{
				GroupID:     "GR2",
				PeriodStart: from.Add(2 * metronome.WindowSize),
				PeriodEnd:   from.Add(3 * metronome.WindowSize),
				SKU:         sku.RemoteCacheCASHits,
				Labels:      internalLabels,
				Count:       11,
			},
		},
	}, client.reported)
}

func TestQueryUsageRows(t *testing.T) {
	env := setupClickHouseEnv(t)
	ctx := t.Context()

	// Add data to clickhouse.
	from := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	to := from.Add(metronome.WindowSize)
	bazelLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client: sku.ClientBazel,
		sku.Origin: sku.OriginExternal,
	}
	internalLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client: sku.ClientBazel,
		sku.Origin: sku.OriginInternal,
	}
	rows := []*schema.RawUsage{
		// Events with the same skus and labels should be aggregated.
		// These events have different buffer IDs so that clickhouse's finalization does not de-duplicate them.
		// This should not be possible because clickhouse should de-duplicate rows, but add test coverage just in case.
		rawUsageWithBufferID("test-1:redis", "GR1", from, sku.BuildEventsBESCount, bazelLabels, 2),
		rawUsageWithBufferID("test-2:redis", "GR1", from, sku.BuildEventsBESCount, bazelLabels, 3),
		// The to-timestamp is exclusive, so this event should not be included.
		rawUsage("GR1", to, sku.BuildEventsBESCount, bazelLabels, 100),
		// Events with the same sku and different labels should be separate.
		rawUsage("GR1", from, sku.RemoteCacheCASHits, bazelLabels, 7),
		rawUsage("GR1", from, sku.RemoteCacheCASHits, internalLabels, 13),
		// Event from a different group.
		rawUsage("GR2", from, sku.BuildEventsBESCount, bazelLabels, 17),
		// Zero count rows do not affect positive aggregates.
		rawUsageWithBufferID("test-3:redis", "GR1", from, sku.BuildEventsBESCount, bazelLabels, 0),
		rawUsageWithBufferID("test-4:redis", "GR1", from, sku.RemoteCacheCASHits, bazelLabels, 0),
		// An aggregate whose rows sum to zero should not be included.
		rawUsage("GR1", from, sku.RemoteCacheCASDownloadedBytes, internalLabels, 0),
	}
	require.NoError(t, env.GetOLAPDBHandle().FlushUsages(ctx, rows))

	allGroups, err := queryUsageRows(ctx, env, nil, &window{from: from, to: to})
	require.NoError(t, err)
	require.ElementsMatch(t, []*schema.Usage{
		{GroupID: "GR1", PeriodStart: from, SKU: sku.BuildEventsBESCount, Labels: bazelLabels, Count: 5},
		{GroupID: "GR1", PeriodStart: from, SKU: sku.RemoteCacheCASHits, Labels: bazelLabels, Count: 7},
		{GroupID: "GR1", PeriodStart: from, SKU: sku.RemoteCacheCASHits, Labels: internalLabels, Count: 13},
		{GroupID: "GR2", PeriodStart: from, SKU: sku.BuildEventsBESCount, Labels: bazelLabels, Count: 17},
	}, allGroups)

	gr1Only, err := queryUsageRows(ctx, env, []string{"GR1"}, &window{from: from, to: to})
	require.NoError(t, err)
	require.ElementsMatch(t, []*schema.Usage{
		{GroupID: "GR1", PeriodStart: from, SKU: sku.BuildEventsBESCount, Labels: bazelLabels, Count: 5},
		{GroupID: "GR1", PeriodStart: from, SKU: sku.RemoteCacheCASHits, Labels: bazelLabels, Count: 7},
		{GroupID: "GR1", PeriodStart: from, SKU: sku.RemoteCacheCASHits, Labels: internalLabels, Count: 13},
	}, gr1Only)
}

func setupClickHouseEnv(t *testing.T) *real_environment.RealEnv {
	flags.Set(t, "olap_database.data_source", testclickhouse.Start(t, true /* reuseServer */))
	flags.Set(t, "olap_database.auto_migrate_db", true)
	flags.Set(t, "olap_database.invocation_batch_insert_interval", 0*time.Second)
	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("metronome_exporter_test"))
	require.NoError(t, clickhouse.Register(env))
	return env
}

func rawUsage(groupID string, periodStart time.Time, usageSKU sku.SKU, labels map[sku.LabelName]sku.LabelValue, count int64) *schema.RawUsage {
	return rawUsageWithBufferID("test:redis", groupID, periodStart, usageSKU, labels, count)
}

func rawUsageWithBufferID(bufferID, groupID string, periodStart time.Time, usageSKU sku.SKU, labels map[sku.LabelName]sku.LabelValue, count int64) *schema.RawUsage {
	return &schema.RawUsage{
		GroupID:     groupID,
		PeriodStart: periodStart,
		SKU:         usageSKU,
		Labels:      orderedmap.FromMap(labels),
		BufferID:    bufferID,
		Count:       count,
	}
}

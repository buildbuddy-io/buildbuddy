package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
)

type fakeMetronomeClient struct {
	reported   [][]metronome.UsageEvent
	failOnCall int
}

func (c *fakeMetronomeClient) ReportUsage(ctx context.Context, events []metronome.UsageEvent) error {
	if c.failOnCall > 0 && len(c.reported)+1 == c.failOnCall {
		return errors.New("metronome unavailable")
	}
	c.reported = append(c.reported, append([]metronome.UsageEvent(nil), events...))
	return nil
}

func TestNextWindow(t *testing.T) {
	latest := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name    string
		lastEnd time.Time
		want    *window
		wantErr bool
	}{
		{
			name:    "behind by less than the max window",
			lastEnd: latest.Add(-3 * metronome.WindowSize),
			want:    &window{from: latest.Add(-3 * metronome.WindowSize), to: latest},
		},
		{
			name:    "behind by more than the max window is capped",
			lastEnd: latest.Add(-3 * time.Hour),
			want:    &window{from: latest.Add(-3 * time.Hour), to: latest.Add(-3 * time.Hour).Add(maxExportWindow)},
		},
		{
			name:    "up to date",
			lastEnd: latest,
			want:    nil,
		},
		{
			name:    "ahead of latest exportable period",
			lastEnd: latest.Add(metronome.WindowSize),
			want:    nil,
		},
		{
			name:    "misaligned state",
			lastEnd: latest.Add(-90 * time.Second),
			wantErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &tables.BillingExportState{LastSuccessfulPeriodEndUsec: tc.lastEnd.UnixMicro()}
			got, err := nextWindow(state, latest)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestExport_InitializesStateOnFirstRun(t *testing.T) {
	env := setupClickHouseEnv(t)
	ctx := t.Context()
	now := time.Date(2026, 6, 8, 12, 0, 30, 0, time.UTC)

	client := &fakeMetronomeClient{}
	require.NoError(t, export(ctx, env, client, now))

	require.Empty(t, client.reported)
	requireState(t, env, now.Add(-minAge).Truncate(metronome.WindowSize))
}

func TestExport_ReportsUsageAndAdvancesState(t *testing.T) {
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

	seedState(t, env, from)
	// Write usage data to clickhouse.
	rows := []*schema.RawUsage{
		rawUsage("GR1", from, sku.BuildEventsBESCount, bazelLabels, 2),
		rawUsage("GR1", from.Add(metronome.WindowSize), sku.BuildEventsBESCount, bazelLabels, 3),
		rawUsage("GR2", from.Add(metronome.WindowSize), sku.RemoteCacheCASHits, internalLabels, 7),
		rawUsage("GR2", from.Add(2*metronome.WindowSize), sku.RemoteCacheCASHits, internalLabels, 11),
		// The to-timestamp is exclusive, so this event should not be reported.
		rawUsage("GR3", to, sku.BuildEventsBESCount, bazelLabels, 100),
		rawUsage("GR4", from, sku.BuildEventsBESCount, bazelLabels, 100),
	}
	require.NoError(t, env.GetOLAPDBHandle().FlushUsages(ctx, rows))

	client := &fakeMetronomeClient{}
	require.NoError(t, export(ctx, env, client, to.Add(minAge)))

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
	requireState(t, env, to)

	client = &fakeMetronomeClient{}
	require.NoError(t, export(ctx, env, client, to.Add(minAge)))
	require.Empty(t, client.reported)
	requireState(t, env, to)
}

func TestExport_CatchesUpOneMaxWindowPerRun(t *testing.T) {
	env := setupClickHouseEnv(t)
	ctx := t.Context()

	from := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	seedState(t, env, from)
	now := from.Add(2 * maxExportWindow).Add(minAge)

	require.NoError(t, export(ctx, env, &fakeMetronomeClient{}, now))
	requireState(t, env, from.Add(maxExportWindow))

	require.NoError(t, export(ctx, env, &fakeMetronomeClient{}, now))
	requireState(t, env, from.Add(2*maxExportWindow))
}

func TestExport_StopsAtFirstFailedWindow(t *testing.T) {
	env := setupClickHouseEnv(t)
	ctx := t.Context()

	from := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	bazelLabels := map[sku.LabelName]sku.LabelValue{
		sku.Client: sku.ClientBazel,
		sku.Origin: sku.OriginExternal,
	}
	seedState(t, env, from)
	rows := []*schema.RawUsage{
		rawUsage("GR1", from, sku.BuildEventsBESCount, bazelLabels, 2),
		rawUsage("GR1", from.Add(metronome.WindowSize), sku.BuildEventsBESCount, bazelLabels, 3),
		rawUsage("GR1", from.Add(2*metronome.WindowSize), sku.BuildEventsBESCount, bazelLabels, 5),
	}
	require.NoError(t, env.GetOLAPDBHandle().FlushUsages(ctx, rows))

	client := &fakeMetronomeClient{failOnCall: 2}
	require.Error(t, export(ctx, env, client, from.Add(3*metronome.WindowSize).Add(minAge)))

	require.Len(t, client.reported, 1)
	requireState(t, env, from.Add(metronome.WindowSize))
}

func TestExport_DryRunDoesNotModifyState(t *testing.T) {
	env := setupClickHouseEnv(t)
	ctx := t.Context()
	now := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)

	require.NoError(t, export(ctx, env, nil /*=client*/, now))
	state, err := loadState(ctx, env)
	require.NoError(t, err)
	require.Nil(t, state)

	from := now.Add(-time.Hour)
	seedState(t, env, from)
	require.NoError(t, env.GetOLAPDBHandle().FlushUsages(ctx, []*schema.RawUsage{
		rawUsage("GR1", from, sku.BuildEventsBESCount, map[sku.LabelName]sku.LabelValue{sku.Client: sku.ClientBazel}, 2),
	}))
	require.NoError(t, export(ctx, env, nil /*=client*/, now))
	requireState(t, env, from)
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

	allGroups, err := queryUsageRows(ctx, env, []string{"GR1", "GR2"}, &window{from: from, to: to})
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
	flags.Set(t, "testenv.use_clickhouse", true)
	flags.Set(t, "testenv.reuse_server", true)
	flags.Set(t, "olap_database.invocation_batch_insert_interval", 0*time.Second)
	env := testenv.GetTestEnv(t)
	for id, status := range map[string]grpb.Group_GroupStatus{
		"GR1": grpb.Group_USAGE_BASED_GROUP_STATUS,
		"GR2": grpb.Group_USAGE_BASED_GROUP_STATUS,
		"GR4": grpb.Group_FREE_TIER_GROUP_STATUS,
	} {
		require.NoError(t, env.GetDBHandle().NewQuery(t.Context(), "test_create_group").Create(&tables.Group{GroupID: id, Status: status}))
	}
	return env
}

func seedState(t *testing.T, env *real_environment.RealEnv, lastEnd time.Time) {
	require.NoError(t, env.GetDBHandle().NewQuery(t.Context(), "test_seed_state").Create(&tables.BillingExportState{
		Destination:                 tables.MetronomeBillingExportDestination,
		LastSuccessfulPeriodEndUsec: lastEnd.UnixMicro(),
	}))
}

func requireState(t *testing.T, env *real_environment.RealEnv, lastEnd time.Time) {
	state, err := loadState(t.Context(), env)
	require.NoError(t, err)
	require.NotNil(t, state)
	require.Equal(t, lastEnd, time.UnixMicro(state.LastSuccessfulPeriodEndUsec).UTC())
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

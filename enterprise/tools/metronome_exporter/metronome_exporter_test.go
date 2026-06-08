package main

import (
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testclickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
)

func TestQueryUsageRows(t *testing.T) {
	env := setupClickHouseEnv(t)
	ctx := t.Context()

	// Add data to clickhouse.
	from := time.Date(2026, 6, 8, 12, 0, 0, 0, time.UTC)
	to := from.Add(5 * time.Minute)
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
		rawUsage("GR1", from, sku.BuildEventsBESCount, bazelLabels, 2),
		rawUsage("GR1", from.Add(time.Minute), sku.BuildEventsBESCount, bazelLabels, 3),
		// The to-timestamp is exclusive, so this event should not be included.
		rawUsage("GR1", to, sku.BuildEventsBESCount, bazelLabels, 100),
		// Events with the same sku and different labels should be separate.
		rawUsage("GR1", from.Add(2*time.Minute), sku.RemoteCacheCASHits, bazelLabels, 7),
		rawUsage("GR1", from.Add(4*time.Minute), sku.RemoteCacheCASHits, internalLabels, 13),
		// Event from a different group.
		rawUsage("GR2", from.Add(time.Minute), sku.BuildEventsBESCount, bazelLabels, 17),
		// Events with zero count should not be included.
		rawUsage("GR1", from.Add(2*time.Minute), sku.BuildEventsBESCount, bazelLabels, 0),
		rawUsage("GR1", from.Add(3*time.Minute), sku.RemoteCacheCASHits, bazelLabels, 0),
	}
	require.NoError(t, env.GetOLAPDBHandle().FlushUsages(ctx, rows))

	allGroups, err := queryUsageRows(ctx, env, nil, &window{from: from, to: to})
	require.NoError(t, err)
	require.ElementsMatch(t, []*schema.Usage{
		{GroupID: "GR1", SKU: sku.BuildEventsBESCount, Labels: bazelLabels, Count: 5},
		{GroupID: "GR1", SKU: sku.RemoteCacheCASHits, Labels: bazelLabels, Count: 7},
		{GroupID: "GR1", SKU: sku.RemoteCacheCASHits, Labels: internalLabels, Count: 13},
		{GroupID: "GR2", SKU: sku.BuildEventsBESCount, Labels: bazelLabels, Count: 17},
	}, allGroups)

	gr1Only, err := queryUsageRows(ctx, env, []string{"GR1"}, &window{from: from, to: to})
	require.NoError(t, err)
	require.ElementsMatch(t, []*schema.Usage{
		{GroupID: "GR1", SKU: sku.BuildEventsBESCount, Labels: bazelLabels, Count: 5},
		{GroupID: "GR1", SKU: sku.RemoteCacheCASHits, Labels: bazelLabels, Count: 7},
		{GroupID: "GR1", SKU: sku.RemoteCacheCASHits, Labels: internalLabels, Count: 13},
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
	return &schema.RawUsage{
		GroupID:     groupID,
		PeriodStart: periodStart,
		SKU:         usageSKU,
		Labels:      orderedmap.FromMap(labels),
		BufferID:    "test:redis",
		Count:       count,
	}
}

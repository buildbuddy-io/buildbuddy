// Metronome usage exporter.
//
// Reads usage data from ClickHouse for usage based groups and ingests
// per-SKU events into Metronome. Designed to run as a scheduled cronjob: each
// run exports the usage recorded since the previous run, tracked in the
// BillingExportState table.
//
// Metronome deduplicates events with the same transaction ID, so re-running
// is safe, if necessary.
//
// Example:
//
//	bazel run //enterprise/tools/metronome_exporter:metronome_exporter \
//	  --database.data_source='mysql://user:password@tcp(mysql:3306)/buildbuddy' \
//	  --olap_database.data_source='clickhouse://default:password@clickhouse:9000/buildbuddy' \
//	  --billing.metronome.api_key=$METRONOME_API_KEY
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	grpb "github.com/buildbuddy-io/buildbuddy/proto/group"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

const (
	// Do not export periods newer than (now - min_age).
	// Prevents querying periods that may still be receiving writes.
	//
	// Usage data might be buffered in redis for up to RedisKeyTTL. It then might take some additional time
	// for the data to be flushed to clickhouse. We add some buffer to ensure all usage data for the period is flushed,
	// before we try to export it to Metronome.
	// Metronome ignores events with duplicate IDs, so if we flush partial usage data for a period, it can't be later amended
	// if we receive more data for that period. This delay ensures all data is finalized before it's flushed.
	minAge = usage.RedisKeyTTL + 10*time.Minute

	// A run exports at most this much usage, so a backlog is drained over
	// several runs.
	maxExportWindow = 1 * time.Hour
)

var (
	dryRun = flag.Bool("dry_run", false, "If true, log what would be sent without calling Metronome or updating the export state.")
)

func main() {
	flag.Parse()
	if err := disableAutoMigration(); err != nil {
		log.Fatalf("disable auto-migration: %s", err)
	}
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := configsecrets.Configure(); err != nil {
		return fmt.Errorf("prepare config secrets provider: %w", err)
	}
	if err := config.Load(); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if err := log.Configure(); err != nil {
		return fmt.Errorf("configure log: %w", err)
	}

	var client usageReporter
	if !*dryRun {
		var err error
		client, err = metronome.NewClient(nil, nil)
		if err != nil {
			return fmt.Errorf("create Metronome client: %w", err)
		}
	}

	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("metronome_exporter"))
	dbh, err := db.GetConfiguredDatabase(ctx, env)
	if err != nil {
		return fmt.Errorf("configure SQL database: %w", err)
	}
	env.SetDBHandle(dbh)
	if err := clickhouse.Register(env); err != nil {
		return fmt.Errorf("configure ClickHouse: %w", err)
	}
	if env.GetOLAPDBHandle() == nil {
		return errors.New("clickhouse database is required")
	}

	return export(ctx, env, client, time.Now())
}

type window struct{ from, to time.Time }

type usageReporter interface {
	ReportUsage(ctx context.Context, events []metronome.UsageEvent) error
}

// A nil client is a dry run: nothing is sent and the export state is not
// modified.
func export(ctx context.Context, env *real_environment.RealEnv, client usageReporter, now time.Time) error {
	latest := now.UTC().Add(-minAge).Truncate(metronome.WindowSize)
	state, err := loadState(ctx, env)
	if err != nil {
		return fmt.Errorf("load export state: %w", err)
	}
	if state == nil {
		log.Infof("No export state found; initializing export to start at %s", latest.Format(time.RFC3339))
		if client == nil {
			return nil
		}
		return env.GetDBHandle().NewQuery(ctx, "metronome_exporter_init_state").Create(&tables.BillingExportState{
			Destination:                 tables.MetronomeBillingExportDestination,
			LastSuccessfulPeriodEndUsec: latest.UnixMicro(),
		})
	}
	w, err := nextWindow(state, latest)
	if err != nil {
		return err
	}
	if w == nil {
		log.Infof("Usage is exported through %s; nothing to do", latest.Format(time.RFC3339))
		return nil
	}
	groups, err := usageBasedGroupIDs(ctx, env)
	if err != nil {
		return fmt.Errorf("list usage based groups: %w", err)
	}
	if len(groups) == 0 {
		log.Infof("No usage based groups; skipping window [%s, %s)", w.from.Format(time.RFC3339), w.to.Format(time.RFC3339))
		return advanceState(ctx, env, client, state, w.to)
	}
	log.Infof("Exporting usage for %d usage based group(s) in window [%s, %s)", len(groups), w.from.Format(time.RFC3339), w.to.Format(time.RFC3339))
	return exportAll(ctx, env, client, groups, w, state)
}

// loadState returns nil if the exporter has not run yet.
func loadState(ctx context.Context, env *real_environment.RealEnv) (*tables.BillingExportState, error) {
	state := &tables.BillingExportState{}
	err := env.GetDBHandle().NewQuery(ctx, "metronome_exporter_load_state").Raw(`
		SELECT * FROM "BillingExportState" WHERE destination = ?`,
		tables.MetronomeBillingExportDestination,
	).Take(state)
	if db.IsRecordNotFound(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return state, nil
}

func advanceState(ctx context.Context, env *real_environment.RealEnv, client usageReporter, state *tables.BillingExportState, end time.Time) error {
	if client == nil {
		return nil
	}
	state.LastSuccessfulPeriodEndUsec = end.UnixMicro()
	if err := env.GetDBHandle().NewQuery(ctx, "metronome_exporter_update_state").Update(state); err != nil {
		return fmt.Errorf("update export state: %w", err)
	}
	return nil
}

// nextWindow returns nil if usage is exported through latest.
func nextWindow(state *tables.BillingExportState, latest time.Time) (*window, error) {
	from := time.UnixMicro(state.LastSuccessfulPeriodEndUsec).UTC()
	if !metronome.IsWindowAligned(from) {
		return nil, fmt.Errorf("export state period end %s is not aligned to %s", from.Format(time.RFC3339Nano), metronome.WindowSize)
	}
	if !from.Before(latest) {
		return nil, nil
	}
	to := latest
	if maxTo := from.Add(maxExportWindow); maxTo.Before(to) {
		to = maxTo
	}
	return &window{from: from, to: to}, nil
}

func usageBasedGroupIDs(ctx context.Context, env *real_environment.RealEnv) ([]string, error) {
	groups, err := db.ScanAll(env.GetDBHandle().NewQuery(ctx, "metronome_exporter_usage_based_groups").Raw(`
		SELECT group_id FROM "Groups" WHERE status = ?`, grpb.Group_USAGE_BASED_GROUP_STATUS,
	), &tables.Group{})
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(groups))
	for _, g := range groups {
		ids = append(ids, g.GroupID)
	}
	return ids, nil
}

func exportAll(ctx context.Context, env *real_environment.RealEnv, client usageReporter, groups []string, w *window, state *tables.BillingExportState) error {
	totalEventCount := 0
	// Export in increments of metronome.WindowSize.
	for start := w.from; start.Before(w.to); start = start.Add(metronome.WindowSize) {
		end := start.Add(metronome.WindowSize)
		rows, err := queryUsageRows(ctx, env, groups, &window{from: start, to: end})
		if err != nil {
			return fmt.Errorf("query ClickHouse: %w", err)
		}
		if len(rows) == 0 {
			if err := advanceState(ctx, env, client, state, end); err != nil {
				return err
			}
			continue
		}
		events := make([]metronome.UsageEvent, 0, len(rows))
		for _, r := range rows {
			events = append(events, metronome.UsageEvent{
				GroupID:     r.GroupID,
				PeriodStart: r.PeriodStart,
				PeriodEnd:   end,
				SKU:         r.SKU,
				Labels:      r.Labels,
				Count:       r.Count,
			})
		}
		if client == nil {
			for _, e := range events {
				log.Infof("DRY-RUN group=%s period=[%s, %s) sku=%s count=%d labels=%v", e.GroupID, e.PeriodStart.Format(time.RFC3339), e.PeriodEnd.Format(time.RFC3339), e.SKU, e.Count, e.Labels)
			}
		} else {
			if err := client.ReportUsage(ctx, events); err != nil {
				return err
			}
		}
		totalEventCount += len(events)
		if err := advanceState(ctx, env, client, state, end); err != nil {
			return err
		}
	}
	log.Infof("Exported %d event(s)", totalEventCount)
	return nil
}

// queryUsageRows returns per-minute usage rows in the window [from, to). The "Usage"
// table is already aggregated by (group_id, period_start, sku, labels) — where
// period_start is a one-minute bucket matching metronome.WindowSize — so this reads
// those rows directly without re-aggregating.
func queryUsageRows(ctx context.Context, env *real_environment.RealEnv, groups []string, w *window) ([]*olaptables.Usage, error) {
	query := `
		SELECT
			group_id,
			sku,
			labels,
			period_start,
			count
		FROM "Usage"
		WHERE period_start >= ?
			AND period_start < ?
			AND group_id IN ?
			AND count > 0
		ORDER BY
			period_start,
			group_id,
			sku`
	rq := env.GetOLAPDBHandle().NewQuery(ctx, "metronome_exporter_query_usage").Raw(query, w.from, w.to, groups)
	return db.ScanAll(rq, &olaptables.Usage{})
}

func disableAutoMigration() error {
	if err := flagutil.SetValueForFlagName("auto_migrate_db", false, nil, false); err != nil {
		return err
	}
	if err := flagutil.SetValueForFlagName("olap_database.auto_migrate_db", false, nil, false); err != nil {
		return err
	}
	return nil
}

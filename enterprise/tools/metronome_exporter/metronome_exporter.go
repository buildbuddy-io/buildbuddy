// Metronome usage exporter.
//
// Reads usage data from ClickHouse for a given time window and ingests
// per-SKU events into Metronome. Designed to run as a scheduled cronjob.
//
// Metronome deduplicates events with the same transaction ID, so re-running
// is safe, if necessary.
//
// Example:
//
//	bazel run //enterprise/tools/metronome_exporter:metronome_exporter \
//	  --from=2026-06-01T00:00:00Z \
//	  --to=2026-06-01T01:01:00Z \
//	  --group_id=GR123 --group_id=GR456 \
//	  --olap_database.data_source='clickhouse://default:password@clickhouse:9000/buildbuddy' \
//	  --billing.metronome.api_key=$METRONOME_API_KEY
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/usage"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

const (
	// Refuse to export if --to is newer than (now - min_age).
	// Prevents querying periods that may still be receiving writes.
	//
	// Usage data might be buffered in redis for up to RedisKeyTTL. It then might take some additional time
	// for the data to be flushed to clickhouse. We add some buffer to ensure all usage data for the period is flushed,
	// before we try to export it to Metronome.
	// Metronome ignores events with duplicate IDs, so if we flush partial usage data for a period, it can't be later amended
	// if we receive more data for that period. This delay ensures all data is finalized before it's flushed.
	minAge = usage.RedisKeyTTL + time.Minute
)

var (
	from     = flag.String("from", "", "Start of the export window (inclusive), RFC3339. Required.")
	to       = flag.String("to", "", "End of the export window (exclusive), RFC3339. Required.")
	groupIDs = flag.Slice("group_id", []string{}, "Group IDs to export. If not set, all groups will be exported.")
	dryRun   = flag.Bool("dry_run", false, "If true, log what would be sent without calling Metronome.")
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

	window, err := parseWindow()
	if err != nil {
		return err
	}
	groups, err := parseGroups()
	if err != nil {
		return err
	}

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
		client, err = metronome.NewClient(nil, nil)
		if err != nil {
			return fmt.Errorf("create Metronome client: %w", err)
		}
	}

	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("metronome_exporter"))
	if err := clickhouse.Register(env); err != nil {
		return fmt.Errorf("configure ClickHouse: %w", err)
	}
	if env.GetOLAPDBHandle() == nil {
		return errors.New("clickhouse database is required")
	}

	groupStr := "all groups"
	if len(groups) > 0 {
		groupStr = strings.Join(groups, ", ")
	}
	log.Infof("Exporting usage for %s in window [%s, %s)", groupStr, window.from.Format(time.RFC3339), window.to.Format(time.RFC3339))
	return exportAll(ctx, env, client, groups, window)
}

type window struct{ from, to time.Time }

type usageReporter interface {
	ReportUsage(ctx context.Context, events []metronome.UsageEvent) error
}

func parseWindow() (*window, error) {
	if *from == "" || *to == "" {
		return nil, errors.New("--from and --to are required")
	}
	f, err := time.Parse(time.RFC3339, *from)
	if err != nil {
		return nil, fmt.Errorf("parse --from: %w", err)
	}
	t, err := time.Parse(time.RFC3339, *to)
	if err != nil {
		return nil, fmt.Errorf("parse --to: %w", err)
	}
	if !f.Before(t) {
		return nil, fmt.Errorf("--from (%s) must be before --to (%s)", f, t)
	}
	cutoff := time.Now().UTC().Add(-minAge)
	if t.After(cutoff) {
		return nil, fmt.Errorf("--to (%s) is within min age %s of now; refusing to export possibly-unsettled periods", t, minAge)
	}
	if !metronome.IsWindowAligned(f) || !metronome.IsWindowAligned(t) {
		return nil, fmt.Errorf("--from and --to must be aligned to window size %s", metronome.WindowSize)
	}
	return &window{from: f, to: t}, nil
}

func parseGroups() ([]string, error) {
	if slices.Contains(*groupIDs, "") {
		return nil, errors.New("--group_id values must be non-empty")
	}
	slices.Sort(*groupIDs)
	deduped := slices.Compact(*groupIDs)
	return deduped, nil
}

func exportAll(ctx context.Context, env *real_environment.RealEnv, client usageReporter, groups []string, w *window) error {
	totalEventCount := 0
	// Export in increments of metronome.WindowSize.
	for start := w.from; start.Before(w.to); start = start.Add(metronome.WindowSize) {
		end := start.Add(metronome.WindowSize)
		rows, err := queryUsageRows(ctx, env, groups, &window{from: start, to: end})
		if err != nil {
			return fmt.Errorf("query ClickHouse: %w", err)
		}
		if len(rows) == 0 {
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
			AND count > 0`
	args := []any{w.from, w.to}
	if len(groups) > 0 {
		query += `
			AND group_id IN ?`
		args = append(args, groups)
	}
	query += `
		ORDER BY
			period_start,
			group_id,
			sku`
	rq := env.GetOLAPDBHandle().NewQuery(ctx, "metronome_exporter_query_usage").Raw(query, args...)
	return db.ScanAll(rq, &olaptables.Usage{})
}

func disableAutoMigration() error {
	if err := flagutil.SetValueForFlagName("olap_database.auto_migrate_db", false, nil, false); err != nil {
		return err
	}
	return nil
}

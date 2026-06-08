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
//	  --to=2026-06-01T01:05:00Z \
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
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
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

const periodGranularity = time.Minute

// Refuse to export if --to is newer than (now - min_age).
// Prevents querying periods that may still be receiving writes.
//
// Usage is bucketed in Redis in 1-min increments, then flushed.
// Metronome ignores events with duplicate IDs, so if we flush partial usage data for a period, it can't be later amended
// if we receive more data for that period. This delay ensures all data is finalized before it's flushed.
const minAge = 5 * time.Minute

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

	var client *metronome.Client
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
	f = f.UTC().Truncate(periodGranularity)
	t = t.UTC().Truncate(periodGranularity)
	if !f.Before(t) {
		return nil, fmt.Errorf("--from (%s) must be before --to (%s)", f, t)
	}
	cutoff := time.Now().UTC().Add(-minAge)
	if t.After(cutoff) {
		return nil, fmt.Errorf("--to (%s) is within min age %s of now; refusing to export possibly-unsettled periods", t, minAge)
	}
	return &window{from: f, to: t}, nil
}

func parseGroups() ([]string, error) {
	seen := map[string]bool{}
	out := make([]string, 0, len(*groupIDs))
	for _, g := range *groupIDs {
		if g == "" {
			return nil, errors.New("--group_id values must be non-empty")
		}
		if seen[g] {
			continue
		}
		seen[g] = true
		out = append(out, g)
	}
	sort.Strings(out)
	return out, nil
}

func exportAll(ctx context.Context, env *real_environment.RealEnv, client *metronome.Client, groups []string, w *window) error {
	rows, err := queryUsageRows(ctx, env, groups, w)
	if err != nil {
		return fmt.Errorf("query ClickHouse: %w", err)
	}
	if len(rows) == 0 {
		return nil
	}
	events := make([]metronome.UsageEvent, 0, len(rows))
	for _, r := range rows {
		events = append(events, metronome.UsageEvent{
			GroupID:     r.GroupID,
			PeriodStart: w.from,
			SKU:         r.SKU,
			Labels:      r.Labels,
			Count:       r.Count,
		})
	}
	if client == nil {
		for _, e := range events {
			log.Infof("DRY-RUN group=%s period=%s sku=%s count=%d labels=%v", e.GroupID, e.PeriodStart.Format(time.RFC3339), e.SKU, e.Count, e.Labels)
		}
		log.Infof("Exported %d event(s)", len(events))
		return nil
	}
	if err := client.ReportUsage(ctx, events); err != nil {
		return err
	}
	log.Infof("Exported %d event(s)", len(events))
	return nil
}

func queryUsageRows(ctx context.Context, env *real_environment.RealEnv, groups []string, w *window) ([]*olaptables.Usage, error) {
	query := `
		SELECT
			group_id,
			sku,
			labels,
			SUM(count) AS count
		FROM "Usage"
		WHERE period_start >= ?
			AND period_start < ?`
	args := []interface{}{w.from, w.to}
	if len(groups) > 0 {
		query += `
			AND group_id IN ?`
		args = append(args, groups)
	}
	query += `
		GROUP BY
			group_id,
			sku,
			labels
		HAVING count > 0
		ORDER BY
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

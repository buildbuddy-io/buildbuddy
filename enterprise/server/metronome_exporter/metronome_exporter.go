// Metronome usage exporter.
//
// Reads usage data from ClickHouse for a given time window and ingests
// per-SKU events into Metronome. Designed to run as a scheduled cronjob:
// the source of truth is ClickHouse, so re-running an overlapping window is
// safe (deterministic transaction IDs cause Metronome to dedupe).
//
// Group IDs to export are passed explicitly via --group_id (repeatable);
// there is no automatic group selection. This is intentional while we roll
// the integration out to a small allowlist.
//
// Example run:
//
//	bb run -- //enterprise/server/metronome_exporter:metronome_exporter \
//	  --from=2026-06-01T00:00:00Z \
//	  --to=2026-06-01T01:00:00Z \
//	  --group_id=GR123 --group_id=GR456 \
//	  --database.data_source='mysql://...' \
//	  --olap_database.data_source='clickhouse://...' \
//	  --billing.metronome.enabled=true \
//	  --billing.metronome.api_key=$METRONOME_API_KEY
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/configsecrets"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"

	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
)

const periodGranularity = time.Minute

var (
	from        = flag.String("from", "", "Start of the export window (inclusive), RFC3339. Required.")
	to          = flag.String("to", "", "End of the export window (exclusive), RFC3339. Required.")
	groupIDs    = flag.Slice("group_id", []string{}, "Group ID to export. May be repeated. At least one required.")
	minAge      = flag.Duration("min_age", 5*time.Minute, "Refuse to export if --to is newer than (now - min_age). Prevents querying periods that may still be receiving writes.")
	concurrency = flag.Int("concurrency", 4, "Max number of groups to export in parallel.")
	dryRun      = flag.Bool("dry_run", false, "If true, log what would be sent without calling Metronome.")
)

func main() {
	if err := disableAutoMigration(); err != nil {
		log.Fatalf("disable auto-migration: %s", err)
	}
	flag.Parse()
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
	if !*dryRun && !metronome.IsConfigured() {
		return errors.New("billing.metronome.enabled is required (or pass --dry_run)")
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

	client := metronome.NewClient(metronome.ClientConfig{})

	log.Infof("Exporting usage for %d group(s) in window [%s, %s)", len(groups), window.from.Format(time.RFC3339), window.to.Format(time.RFC3339))
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
	cutoff := time.Now().UTC().Add(-*minAge)
	if t.After(cutoff) {
		return nil, fmt.Errorf("--to (%s) is within --min_age=%s of now; refusing to export possibly-unsettled periods", t, *minAge)
	}
	return &window{from: f, to: t}, nil
}

func parseGroups() ([]string, error) {
	if len(*groupIDs) == 0 {
		return nil, errors.New("at least one --group_id is required")
	}
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
	var (
		mu       sync.Mutex
		failed   []string
		eventCnt int
	)
	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(*concurrency)
	for _, gid := range groups {
		gid := gid
		eg.Go(func() error {
			n, err := exportGroup(ctx, env, client, gid, w)
			mu.Lock()
			defer mu.Unlock()
			eventCnt += n
			if err != nil {
				log.Errorf("group %s: export failed: %s", gid, err)
				failed = append(failed, gid)
				return nil // don't cancel siblings; partial success is acceptable
			}
			log.Infof("group %s: exported %d event(s)", gid, n)
			return nil
		})
	}
	_ = eg.Wait()
	log.Infof("Exported %d event(s) across %d group(s); %d group(s) failed", eventCnt, len(groups), len(failed))
	if len(failed) > 0 {
		return fmt.Errorf("%d group(s) failed: %v", len(failed), failed)
	}
	return nil
}

func exportGroup(ctx context.Context, env *real_environment.RealEnv, client *metronome.Client, gid string, w *window) (int, error) {
	rows, err := queryUsageRows(ctx, env, gid, w)
	if err != nil {
		return 0, fmt.Errorf("query ClickHouse: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}
	events := make([]metronome.Event, 0, len(rows))
	for _, r := range rows {
		events = append(events, metronome.Event{
			GroupID:     r.GroupID,
			PeriodStart: r.PeriodStart,
			SKU:         r.SKU,
			Labels:      r.Labels,
			Count:       r.Count,
			Unit:        sku.UnitFor(r.SKU),
		})
	}
	if *dryRun {
		for _, e := range events {
			log.Infof("DRY-RUN group=%s period=%s sku=%s count=%d labels=%v", e.GroupID, e.PeriodStart.Format(time.RFC3339), e.SKU, e.Count, e.Labels)
		}
		return len(events), nil
	}
	if err := client.IngestEvents(ctx, events); err != nil {
		return 0, err
	}
	return len(events), nil
}

func queryUsageRows(ctx context.Context, env *real_environment.RealEnv, gid string, w *window) ([]olaptables.Usage, error) {
	var rows []olaptables.Usage
	err := env.GetOLAPDBHandle().GORM(ctx, "metronome_exporter_query_usage").
		Table((&olaptables.Usage{}).ViewName()).
		Where("group_id = ? AND period_start >= ? AND period_start < ?", gid, w.from, w.to).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
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

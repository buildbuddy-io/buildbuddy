// Metronome usage exporter.
//
// Reads usage data from ClickHouse for selected groups and
// ingests per-SKU events into Metronome. Designed to run as a scheduled
// cronjob: each run exports the usage recorded since the previous run, tracked
// in the BillingExportState table. A group gets a Metronome customer and
// contract the first time it reports usage.
//
// Metronome deduplicates events with the same transaction ID, so re-running
// is safe, if necessary.
//
// Example:
//
//	bazel run //enterprise/tools/metronome_exporter:metronome_exporter \
//	  --group_ids=GR_CUSTOMER_ID \
//	  --auto_migrate_db=false \
//	  --database.data_source='mysql://user:password@tcp(mysql:3306)/buildbuddy' \
//	  --olap_database.data_source='clickhouse://default:password@clickhouse:9000/buildbuddy' \
//	  --billing.metronome.api_key=$METRONOME_API_KEY
//	  --billing.metronome.default_contract_name=<CONTRACT_NAME>
//
// To select groups by status instead, use e.g.
// --group_statuses='[1,5]' (free tier and usage based).
// When both selectors are set, groups matching either are exported. At least
// one selector is required.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode"

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
)

var (
	dryRun        = flag.Bool("dry_run", false, "If true, log what would be sent without calling Metronome or updating the export state.")
	groupIDs      = flag.Slice("group_ids", []string{}, "Comma-separated group IDs to export, regardless of status. Combined with group_statuses using OR. At least one selector is required.")
	groupStatuses = flag.Slice("group_statuses", []grpb.Group_GroupStatus{}, "JSON array of integer GroupStatus enum values to export (e.g. [1,5] for free tier and usage based). Combined with group_ids using OR. At least one selector is required.")
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

	if err := validateGroupSelection(); err != nil {
		return err
	}

	var client metronomeClient
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

type metronomeClient interface {
	ReportUsage(ctx context.Context, events []metronome.UsageEvent) error
	FindCustomerID(ctx context.Context, ingestAlias string) (string, error)
	CreateCustomer(ctx context.Context, name, ingestAlias string) (string, error)
	HasContract(ctx context.Context, customerID string, usageTime time.Time) (bool, error)
	CreateContract(ctx context.Context, customerID, contractName string, startingAt time.Time, uniquenessKey string) error
}

// A nil client is a dry run: nothing is sent and the export state is not
// modified.
func export(ctx context.Context, env *real_environment.RealEnv, client metronomeClient, now time.Time) error {
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
	groups, err := exportedGroups(ctx, env)
	if err != nil {
		return fmt.Errorf("list groups: %w", err)
	}
	if len(groups) == 0 {
		log.Infof("No groups to export; skipping window [%s, %s)", w.from.Format(time.RFC3339), w.to.Format(time.RFC3339))
		return advanceState(ctx, env, client, state, w.to)
	}
	log.Infof("Exporting usage for %d group(s) in window [%s, %s)", len(groups), w.from.Format(time.RFC3339), w.to.Format(time.RFC3339))
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

func advanceState(ctx context.Context, env *real_environment.RealEnv, client metronomeClient, state *tables.BillingExportState, end time.Time) error {
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
	return &window{from: from, to: latest}, nil
}

func validateGroupSelection() error {
	if len(*groupIDs) == 0 && len(*groupStatuses) == 0 {
		return errors.New("at least one of group_ids or group_statuses must be configured")
	}
	for i, id := range *groupIDs {
		id = strings.TrimSpace(id)
		if id == "" {
			return errors.New("group_ids must not contain empty IDs")
		}
		if strings.ContainsFunc(id, unicode.IsSpace) {
			return fmt.Errorf("group_ids must not contain whitespace within IDs: %q", id)
		}
		(*groupIDs)[i] = id
	}
	for _, value := range *groupStatuses {
		if _, ok := grpb.Group_GroupStatus_name[int32(value)]; !ok || value == grpb.Group_UNKNOWN_GROUP_STATUS {
			return fmt.Errorf("unknown group_statuses value %d; expected a GroupStatus enum value", value)
		}
	}
	return nil
}

func exportedGroups(ctx context.Context, env *real_environment.RealEnv) ([]*tables.Group, error) {
	var clauses []string
	var args []any
	if len(*groupIDs) > 0 {
		clauses = append(clauses, "group_id IN ?")
		args = append(args, *groupIDs)
	}
	if len(*groupStatuses) > 0 {
		clauses = append(clauses, "status IN ?")
		args = append(args, *groupStatuses)
	}
	query := `SELECT group_id, status FROM "Groups" WHERE ` + strings.Join(clauses, " OR ")
	groups, err := db.ScanAll(env.GetDBHandle().NewQuery(ctx, "metronome_exporter_groups").Raw(query, args...), &tables.Group{})
	if err != nil {
		return nil, err
	}
	found := make(map[string]struct{}, len(groups))
	for _, g := range groups {
		found[g.GroupID] = struct{}{}
	}
	for _, id := range *groupIDs {
		if _, ok := found[id]; !ok {
			log.Warningf("requested group ID %q does not exist", id)
		}
	}
	return groups, nil
}

func exportAll(ctx context.Context, env *real_environment.RealEnv, client metronomeClient, groups []*tables.Group, w *window, state *tables.BillingExportState) error {
	ids := make([]string, 0, len(groups))
	groupsByID := make(map[string]*tables.Group, len(groups))
	for _, group := range groups {
		ids = append(ids, group.GroupID)
		groupsByID[group.GroupID] = group
	}
	customersChecked := map[string]bool{}
	totalEventCount := 0
	// Export in increments of metronome.WindowSize.
	for start := w.from; start.Before(w.to); start = start.Add(metronome.WindowSize) {
		end := start.Add(metronome.WindowSize)
		rows, err := queryUsageRows(ctx, env, ids, &window{from: start, to: end})
		if err != nil {
			return fmt.Errorf("query ClickHouse: %w", err)
		}
		if len(rows) > 0 {
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
				for _, r := range rows {
					if customersChecked[r.GroupID] {
						continue
					}
					customersChecked[r.GroupID] = true
					// Not fatal: Metronome matches the usage to the customer
					// once a later run creates it.
					if err := ensureCustomer(ctx, client, groupsByID[r.GroupID], start); err != nil {
						log.Warningf("Failed to set up Metronome customer for group %s: %s", r.GroupID, err)
					}
				}
				if err := client.ReportUsage(ctx, events); err != nil {
					return err
				}
			}
			totalEventCount += len(events)
		}
		if err := advanceState(ctx, env, client, state, end); err != nil {
			return err
		}
	}
	log.Infof("Exported %d event(s)", totalEventCount)
	return nil
}

func ensureCustomer(ctx context.Context, client metronomeClient, group *tables.Group, usageTime time.Time) error {
	groupID := group.GroupID
	customerID, err := client.FindCustomerID(ctx, groupID)
	if err != nil {
		return err
	}
	if customerID == "" {
		customerID, err = client.CreateCustomer(ctx, groupID, groupID)
		if err != nil {
			return err
		}
		log.Infof("Created Metronome customer %s for group %s", customerID, groupID)
	}

	hasContract, err := client.HasContract(ctx, customerID, usageTime)
	if err != nil {
		return fmt.Errorf("check existing contract: %w", err)
	}
	if hasContract {
		return nil
	}
	if group.Status != grpb.Group_FREE_TIER_GROUP_STATUS && group.Status != grpb.Group_USAGE_BASED_GROUP_STATUS {
		return fmt.Errorf("automatic contract creation is only supported for free tier and usage based groups, failed on group %s", groupID)
	}

	if metronome.DefaultContractName() == "" {
		return fmt.Errorf("--billing.metronome.default_contract_name is required to create contracts by default, failed on group %s", groupID)
	}
	monthStart := time.Date(usageTime.Year(), usageTime.Month(), 1, 0, 0, 0, 0, time.UTC)
	return client.CreateContract(ctx, customerID, metronome.DefaultContractName(), monthStart, groupID)
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

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
)

const (
	dryRun = true
	limit  = 1000

	batchSize         = 1000
	updateConcurrency = 100
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	flag.Parse()

	for k, v := range map[string]any{
		"auto_migrate_db":                    false,
		"database.max_open_conns":            100,
		"database.max_idle_conns":            25,
		"database.conn_max_lifetime_seconds": 300,
	} {
		if err := flagutil.SetValueForFlagName(k, v, nil, false); err != nil {
			return err
		}
	}
	if err := log.Configure(); err != nil {
		return err
	}

	ctx := context.Background()
	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker(""))
	go func() {
		env.GetHealthChecker().WaitForGracefulShutdown()
		os.Exit(0)
	}()
	dbh, err := db.GetConfiguredDatabase(ctx, env)
	if err != nil {
		return err
	}

	n := 0
	eg := &errgroup.Group{}
	defer eg.Wait()

	updates := make(chan *tables.Usage, 1000)
	defer close(updates)

	for i := 0; i < updateConcurrency; i++ {
		eg.Go(func() error {
			for u := range updates {
				if dryRun {
					log.Infof("Would update %+v", u)
					continue
				}
				err := dbh.NewQuery(ctx, "usage_backfill_update").Raw(`
					UPDATE "Usages"
					SET usage_id = ?
					WHERE period_start_usec = ?
					AND region = ?
					AND group_id = ?
					AND origin = ?
					AND client = ?
					`,
					u.UsageID,
					u.PeriodStartUsec,
					u.Region,
					u.GroupID,
					u.Origin,
					u.Client,
				).Exec().Error
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	// Process groups separately so we can use the group_id/period index.
	var gids []string
	log.Infof("Getting groups")
	q := dbh.NewQuery(ctx, "usage_backfill_get_group_ids").Raw(`
		SELECT DISTINCT group_id
		FROM "Usages"
	`)
	rows, err := db.ScanAll(q, &tables.Usage{})
	if err != nil {
		return fmt.Errorf("scan group IDs: %w", err)
	}
	for _, r := range rows {
		if r.GroupID == "" {
			return fmt.Errorf("row is missing group_id?")
		}
		gids = append(gids, r.GroupID)
	}

	for _, gid := range gids {
		log.Infof("Backfilling group %s", gid)

		// Keep track of the period_start_usec of the row we last updated, so we
		// don't need to re-scan rows that we already updated.
		// Start at 2023-11-01 00:00:00 UTC at which point we were definitely
		// writing usage_ids.
		var periodStartUsec int64 = 1698796800000000

		for {
			q := dbh.NewQuery(ctx, "usage_backfill_select").Raw(`
				SELECT * FROM "Usages"
				WHERE (usage_id = '' OR usage_id IS NULL)
				AND period_start_usec <= ?
				AND group_id = ?
				ORDER BY period_start_usec DESC
				LIMIT ?
			`, periodStartUsec, gid, batchSize)
			rows, err := db.ScanAll(q, &tables.Usage{})
			if err != nil {
				return fmt.Errorf("select rows to update: %w", err)
			}
			for _, u := range rows {
				pk, err := tables.PrimaryKeyForTable(u.TableName())
				if err != nil {
					return err
				}
				u.UsageID = pk
				updates <- u
				periodStartUsec = u.PeriodStartUsec
				n++
				if limit > 0 && n >= limit {
					break
				}
			}
			log.Infof("Processed %d rows so far; next period_start_usec <= %d", n, periodStartUsec)
			if limit > 0 && n >= limit {
				log.Infof("Reached limit; exiting.")
				return nil
			}
			if len(rows) < batchSize {
				log.Infof("Found %d rows in last round (less than batch size %d); advancing to next group.", len(rows), batchSize)
				break
			}
		}
	}

	return nil
}

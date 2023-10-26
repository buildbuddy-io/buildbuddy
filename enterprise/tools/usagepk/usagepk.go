package main

import (
	"context"
	"os"
	"time"

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
	dryRun            = false
	limit             = 0
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
	if err := flagutil.SetValueForFlagName("auto_migrate_db", false, nil, false); err != nil {
		return err
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

	d := dbh.DB(ctx)
	// Keep track of the period_start_usec of the row we last updated, so we
	// don't need to re-scan rows that we already updated.
	periodStartUsec := time.Now().UnixMicro()
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
				err := d.Exec(`
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
				).Error
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	for {
		rows, err := d.Raw(`
			SELECT * FROM "Usages"
			WHERE (usage_id = '' OR usage_id IS NULL)
			AND period_start_usec <= ?
			ORDER BY period_start_usec DESC
			LIMIT ?
		`, periodStartUsec, batchSize).Rows()
		if err != nil {
			return err
		}
		r := 0
		for rows.Next() {
			u := &tables.Usage{}
			if err := d.ScanRows(rows, u); err != nil {
				return err
			}
			pk, err := tables.PrimaryKeyForTable(u.TableName())
			if err != nil {
				return err
			}
			u.UsageID = pk
			updates <- u
			periodStartUsec = u.PeriodStartUsec
			r++
			n++
			if limit > 0 && n >= limit {
				break
			}
		}
		if r == 0 {
			log.Infof("Updated 0 rows in last round; exiting.")
			return nil
		}
		log.Infof("Processed %d rows so far; next period_start_usec <= %d", n, periodStartUsec)
		if limit > 0 && n >= limit {
			log.Infof("Reached limit; exiting.")
			return nil
		}
		if dryRun {
			log.Infof("Dry-run: exiting after first round of updates.")
			return nil
		}
	}
}

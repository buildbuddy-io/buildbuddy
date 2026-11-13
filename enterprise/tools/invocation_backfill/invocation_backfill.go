// Tool to backfill missing invocation rows from MySQL to ClickHouse.
//
//  1. Run `kubectl port-forward` for both mysql (readonly replica is fine) and
//     clickhouse (read+write).
//  2. Figure out the start/end times for the backfill and convert to
//     unix microseconds.
//  3. Do a dry-run:
//     bazel run -- //enterprise/tools/invocation_backfill \
//     --olap_database.data_source=clickhouse://localhost:9000/buildbuddy_prod \
//     --database.data_source=mysql://localhost:3308/buildbuddy_prod \
//     --start_usec=XXXXXXXX \
//     --end_usec=YYYYYYYY
//  4. If everything looks good, run the tool again with --dry_run=false to
//     actually write the data to ClickHouse.
package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	startUsec = flag.Int64("start_usec", 0, "The start time to backfill invocations from (in microseconds)")
	endUsec   = flag.Int64("end_usec", 0, "The end time to backfill invocations to (in microseconds)")
	groupID   = flag.String("group_id", "", "If set, restrict to this group ID")
	dryRun    = flag.Bool("dry_run", true, "If true, don't write anything, just log what would be written")
)

const (
	// Number of invocation IDs to query at once when querying for invocation
	// IDs missing from ClickHouse.
	findMissingBatchSize = 100

	// Number of workers to query missing invocation IDs from ClickHouse.
	numFindMissingWorkers = 2

	// Number of invocations to read at once when reading the full missing
	// invocation rows from MySQL and writing them to ClickHouse.
	copyBatchSize = 2500

	// Number of workers to read the full missing invocation rows from MySQL
	// and write them to ClickHouse.
	numCopyWorkers = 2

	clickhouseInsertRateLimit = 1 // insert(s) per second
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	if err := config.Load(); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if err := flagutil.SetValueForFlagName("auto_migrate_db", false, nil, false); err != nil {
		return fmt.Errorf("disable primary DB auto-migration: %w", err)
	}
	if err := flagutil.SetValueForFlagName("olap_database.auto_migrate_db", false, nil, false); err != nil {
		return fmt.Errorf("disable OLAP DB auto-migration: %w", err)
	}
	if err := flagutil.SetValueForFlagName("database.slow_query_threshold", 1*time.Minute, nil, false); err != nil {
		return fmt.Errorf("set slow query threshold: %w", err)
	}

	if *startUsec == 0 || *endUsec == 0 {
		return fmt.Errorf("Both start_usec and end_usec must be set")
	}

	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("invocation_backfill"))
	ctx := env.GetServerContext()

	// Register primary DB
	dbHandle, err := db.GetConfiguredDatabase(ctx, env)
	if err != nil {
		return fmt.Errorf("configure database: %w", err)
	}
	env.SetDBHandle(dbHandle)

	// Register clickhouse DB
	if err := clickhouse.Register(env); err != nil {
		return fmt.Errorf("register clickhouse db: %w", err)
	}
	if env.GetOLAPDBHandle() == nil {
		return fmt.Errorf("clickhouse db not configured")
	}

	eg, ctx := errgroup.WithContext(ctx)

	// Start a worker to get all invocation IDs in the backfill range,
	// group-by-group.
	type groupInvocationBatch struct {
		GroupID     string
		Invocations []*tables.Invocation
	}
	groupInvocationBatches := make(chan *groupInvocationBatch, 10)
	eg.Go(func() error {
		defer close(groupInvocationBatches)

		var groups []*tables.Invocation
		if *groupID == "" {
			// Get all distinct group IDs first, so we can filter by group ID. For
			// some reason, this is needed in order to make the invocation ID
			// queries fast, even though we have an index on (group_id,
			// updated_at_usec) and not that many groups.
			q := env.GetDBHandle().NewQuery(ctx, "invocation_backfill_get_distinct_group_ids").Raw(`
				SELECT DISTINCT group_id
				FROM Invocations
				WHERE group_id IS NOT NULL
			`)
			log.Infof("Querying all group IDs")
			rows, err := db.ScanAll(q, &tables.Invocation{})
			if err != nil {
				return fmt.Errorf("get distinct group IDs: %w", err)
			}
			groups = rows
			log.Infof("Backfilling invocations for %d groups", len(groups))
		} else {
			groups = []*tables.Invocation{{GroupID: *groupID}}
			log.Infof("Backfilling invocations for group %s", *groupID)
		}

		for _, group := range groups {
			var batch *groupInvocationBatch

			q := env.GetDBHandle().NewQuery(ctx, "invocation_backfill_get_invocation_ids").Raw(`
				SELECT invocation_uuid, updated_at_usec, group_id
				FROM Invocations
				WHERE updated_at_usec >= ? AND updated_at_usec < ?
				AND group_id = ?
				ORDER BY updated_at_usec ASC
			`, *startUsec, *endUsec, group.GroupID)
			err := q.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
				// invocation_uuid is stored as []byte in the DB, which is
				// compatible with uuid.UUID, defined as [16]byte.
				invocation := &tables.Invocation{}
				if err := row.Scan(&invocation.InvocationUUID, &invocation.UpdatedAtUsec, &invocation.GroupID); err != nil {
					return err
				}
				if len(invocation.InvocationUUID) != 16 {
					return fmt.Errorf("invocation UUID length %d != 16", len(invocation.InvocationUUID))
				}

				// Init batch if needed, append invocation to batch, and flush
				// the batch if it's full.
				if batch == nil {
					batch = &groupInvocationBatch{
						GroupID:     group.GroupID,
						Invocations: make([]*tables.Invocation, 0, findMissingBatchSize),
					}
				}
				batch.Invocations = append(batch.Invocations, invocation)
				if len(batch.Invocations) == findMissingBatchSize {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case groupInvocationBatches <- batch:
						// Reset batch
						batch = nil
					}
				}

				return nil
			})
			if err != nil {
				return fmt.Errorf("scan all invocation IDs in backfill range for group %s: %w", group.GroupID, err)
			}
			if batch != nil {
				// Flush final batch for the group
				select {
				case <-ctx.Done():
					return ctx.Err()
				case groupInvocationBatches <- batch:
					batch = nil
				}
			}
		}
		return nil
	})

	// Start workers to query the missing invocation IDs from ClickHouse
	missingInvocationIDs := make(chan uuid.UUID, copyBatchSize*numCopyWorkers)
	var findMissingEG errgroup.Group
	for range numFindMissingWorkers {
		findMissingEG.Go(func() error {
			for batch := range groupInvocationBatches {
				hexUUIDs := toHexUUIDs(getInvocationUUIDs(batch.Invocations))
				missingHexUUIDs := set.From(hexUUIDs...)
				minUpdatedAtUsec, maxUpdatedAtUsec := getMinMaxUpdatedAtUsec(batch.Invocations)
				q := env.GetOLAPDBHandle().NewQuery(ctx, "invocation_backfill_find_missing_invocation_ids").Raw(`
					SELECT invocation_uuid
					FROM Invocations
					WHERE updated_at_usec >= ? AND updated_at_usec <= ?
					AND group_id = ?
					AND invocation_uuid IN (?)
				`, minUpdatedAtUsec, maxUpdatedAtUsec, batch.GroupID, hexUUIDs)
				err := q.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
					var invocationUUID string
					if err := row.Scan(&invocationUUID); err != nil {
						return err
					}
					missingHexUUIDs.Remove(invocationUUID)
					return nil
				})
				if err != nil {
					return fmt.Errorf("find missing invocation IDs from ClickHouse: %w", err)
				}
				log.Infof("%3d of %3d invocations in batch are missing from ClickHouse", len(missingHexUUIDs), len(batch))
				for hexUUID := range missingHexUUIDs {
					// Convert hex string to UUID
					b, err := hex.DecodeString(hexUUID)
					if err != nil {
						return fmt.Errorf("decode hex UUID: %w", err)
					}
					uuid := uuid.UUID(b)
					select {
					case <-ctx.Done():
						return ctx.Err()
					case missingInvocationIDs <- uuid:
					}
				}
			}
			return nil
		})
	}
	eg.Go(func() error {
		defer close(missingInvocationIDs)
		return findMissingEG.Wait()
	})

	// Start a worker to batch up the missing invocation IDs for more efficient
	// querying from MySQL.
	missingInvocationIDBatches := make(chan []uuid.UUID, numCopyWorkers)
	eg.Go(func() error {
		defer close(missingInvocationIDBatches)
		var batch []uuid.UUID
		for iid := range missingInvocationIDs {
			batch = append(batch, iid)
			if len(batch) == copyBatchSize {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case missingInvocationIDBatches <- batch:
				}
				batch = nil
			}
		}
		if len(batch) > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case missingInvocationIDBatches <- batch:
				missingInvocationIDBatches <- batch
			}
		}
		return nil
	})

	// Start workers to read the full missing invocation rows from MySQL
	// and write them to ClickHouse.
	insertRateLimiter := rate.NewLimiter(rate.Limit(clickhouseInsertRateLimit), 1)
	for range numCopyWorkers {
		eg.Go(func() error {
			for batch := range missingInvocationIDBatches {
				log.Infof("Copying batch of %d missing invocation rows from MySQL to ClickHouse", len(batch))

				var uuidPredicates []string
				var uuidValues []any
				for _, uuid := range batch {
					uuidPredicates = append(uuidPredicates, "invocation_uuid = UNHEX(REPLACE(?, '-', ''))")
					uuidValues = append(uuidValues, uuid.String())
				}

				q := env.GetDBHandle().NewQuery(ctx, "invocation_backfill_read_missing_invocation_rows").Raw(`
					SELECT * FROM Invocations
					WHERE ( `+strings.Join(uuidPredicates, " OR ")+` )
					AND updated_at_usec >= ? AND updated_at_usec < ?
				`, append(uuidValues, *startUsec, *endUsec)...)
				invocations, err := db.ScanAll(q, &tables.Invocation{})
				if err != nil {
					return fmt.Errorf("read missing invocation rows from MySQL: %w", err)
				}

				// Sanity check: make sure we found all the invocations.
				if len(invocations) != len(batch) {
					missing := set.From(toStringUUIDs(batch)...)
					for _, invocation := range invocations {
						missing.Remove(uuid.UUID(invocation.InvocationUUID).String())
					}
					log.Warningf("Scanned invocations length %d != batch length %d", len(invocations), len(batch))
					log.Warningf("Missing invocations: %s", strings.Join(slices.Collect(missing.All()), ", "))
				}

				olapInvocations := make([]*schema.Invocation, len(invocations))
				for i, invocation := range invocations {
					olapInvocations[i] = schema.ToInvocationFromPrimaryDB(invocation)
				}
				if err := insertRateLimiter.Wait(ctx); err != nil {
					return fmt.Errorf("wait for insert rate limit: %w", err)
				}

				if *dryRun {
					log.Infof("[DRY RUN] would write a batch of %d invocations to ClickHouse. First item in batch: %+#v", len(olapInvocations), olapInvocations[0])
					continue
				}
				if err := env.GetOLAPDBHandle().GORM(ctx, "invocation_backfill_write_missing_invocation_rows").Create(olapInvocations).Error; err != nil {
					return fmt.Errorf("write missing invocation rows to ClickHouse: %w", err)
				}
				log.Infof("Copied batch of %d invocations to ClickHouse", len(olapInvocations))
			}
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("worker failed: %w", err)
	}
	return nil
}

func toStringUUIDs(uuids []uuid.UUID) []string {
	out := make([]string, len(uuids))
	for i, uuid := range uuids {
		out[i] = uuid.String()
	}
	return out
}

func toHexUUIDs(uuids []uuid.UUID) []string {
	out := make([]string, len(uuids))
	for i, uuid := range uuids {
		out[i] = hex.EncodeToString(uuid[:])
	}
	return out
}

func getInvocationUUIDs(invocations []*tables.Invocation) []uuid.UUID {
	out := make([]uuid.UUID, len(invocations))
	for i, invocation := range invocations {
		out[i] = uuid.UUID(invocation.InvocationUUID)
	}
	return out
}

func getMinMaxUpdatedAtUsec(invocations []*tables.Invocation) (int64, int64) {
	minUpdatedAtUsec := int64(math.MaxInt64)
	maxUpdatedAtUsec := int64(0)
	for _, invocation := range invocations {
		minUpdatedAtUsec = min(minUpdatedAtUsec, invocation.UpdatedAtUsec)
		maxUpdatedAtUsec = max(maxUpdatedAtUsec, invocation.UpdatedAtUsec)
	}
	return minUpdatedAtUsec, maxUpdatedAtUsec
}

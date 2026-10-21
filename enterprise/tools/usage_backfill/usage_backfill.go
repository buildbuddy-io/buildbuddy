// Package main implements the usage_backfill command, which copies legacy
// MySQL Usages aggregates into ClickHouse RawUsage rows.
//
// If ClickHouse RawUsage already has rows, the tool finds the earliest
// period_start currently present, snaps that timestamp to the start of its UTC
// month, then walks backwards from the previous month until the oldest MySQL
// usage period. This tool assumes operators have dropped any RawUsage
// partitions that should be rebuilt, so it never rewrites the earliest
// ClickHouse month that still exists. If RawUsage is empty, the tool uses the
// start of next UTC month as the boundary, so the first copied window is the
// current partial UTC month. At startup, the tool reads each MySQL group's
// oldest and newest usage period from the group_period_region_index_v2 index.
// Each month skips groups whose usage range cannot overlap the month, queries
// the remaining groups concurrently, aggregates duplicate rows by
// period_start_usec, group_id, region, origin, client, and server, then expands
// every positive MySQL counter into one or more RawUsage rows.
// MySQL period_start_usec is preserved as RawUsage period_start. The tool does
// not infer the historical period duration or split legacy hourly rows across
// minute periods; old hourly rows are attributed to their hour start timestamp,
// and newer minute rows are attributed to their minute start timestamp.
//
// Row translation summary:
//
//   - invocations becomes sku.BuildEventsBESCount.
//   - cas_cache_hits becomes sku.RemoteCacheCASHits.
//   - action_cache_hits becomes sku.RemoteCacheACHits.
//   - total_download_size_bytes becomes sku.RemoteCacheCASDownloadedBytes.
//   - total_upload_size_bytes becomes sku.RemoteCacheCASUploadedBytes.
//   - total_cached_action_exec_usec becomes
//     sku.RemoteCacheACCachedExecDurationNanos after multiplying by 1,000.
//   - linux_execution_duration_usec, mac_execution_duration_usec,
//     self_hosted_linux_execution_duration_usec, and
//     self_hosted_mac_execution_duration_usec all become
//     sku.RemoteExecutionExecuteWorkerDurationNanos after multiplying by 1,000.
//     These rows add os and self_hosted labels to distinguish the source MySQL
//     columns.
//   - cpu_nanos becomes sku.RemoteExecutionExecuteWorkerCPUNanos with linux
//     and self_hosted=false execution labels.
//   - memory_gb_usec becomes sku.RemoteExecutionExecuteWorkerMemoryGBNanos
//     after multiplying by 1,000, also with linux and self_hosted=false
//     execution labels.
//   - *_execution_compute_duration_usec fields become
//     sku.RemoteExecutionExecuteFixedComputeNanos after multiplying by 1,000.
//   - *_execution_burstable_compute_duration_usec fields become
//     sku.RemoteExecutionExecuteFlexibleComputeNanos after multiplying by 1,000.
//     These compute rows add os, arch, and self_hosted=false labels.
//
// The base RawUsage labels come from the MySQL origin, client, and server
// columns. Empty label values are omitted. The RawUsage buffer_id is hard coded
// to "<mysql region>:redis" to match production usage buffering.
//
// MySQL reads run through 32 workers. Each read task queries one group in one
// month, and the tool completes a whole month before moving to the previous
// month. Before querying monthly usage, workers memoize whether the group has
// any usage in that year, which avoids probing every month in long min/max
// gaps. MySQL workers stream converted RawUsage rows to a collector, which
// builds ClickHouse batches scoped to the current month. The batch size is
// controlled by -insert_batch_size and defaults to 100K RawUsage rows. Four
// ClickHouse writer workers flush those batches with PrepareBatch. Reruns are
// harmless only if operators first drop the whole RawUsage month partitions
// that should be rebuilt.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column/orderedmap"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
)

var (
	dryRun          = flag.Bool("dry_run", false, "Print the first few backfill months and row counts without inserting into ClickHouse")
	insertBatchSize = flag.Int("insert_batch_size", 100_000, "Number of RawUsage rows to insert per ClickHouse batch")
)

const (
	dryRunMaxMonths         = 3
	mysqlQueryWorkers       = 32
	clickHouseWriterWorkers = 4
)

type usageGroupRange struct {
	GroupID            string
	MinPeriodStartUsec int64
	MaxPeriodStartUsec int64
}

type backfillMonth struct {
	StartTime   time.Time
	EndTime     time.Time
	GroupRanges []usageGroupRange
}

type mysqlQueryTask struct {
	Month      backfillMonth
	GroupRange usageGroupRange
}

type mysqlQueryResult struct {
	GroupID      string
	MySQLRows    int
	RawUsageRows []schema.RawUsage
	Skipped      bool
}

type usageAggregateKey struct {
	PeriodStartUsec int64
	GroupID         string
	Region          string
	Origin          string
	Client          string
	Server          string
}

// usageYearCache memoizes sparse group and year existence checks so long
// min/max group spans do not force full usage queries for empty years.
type usageYearCache struct {
	clickHouseBoundary time.Time
	mu                 sync.Mutex
	yearValues         map[groupYearKey]bool
	group              singleflight.Group
}

type groupYearKey struct {
	GroupID string
	Year    int
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	if *insertBatchSize <= 0 {
		return fmt.Errorf("invalid -insert_batch_size %d: must be positive", *insertBatchSize)
	}
	mysqlDataSource, err := flagValue("database.data_source")
	if err != nil {
		return err
	}
	clickhouseDataSource, err := flagValue("olap_database.data_source")
	if err != nil {
		return err
	}

	ctx := context.Background()
	mysqlDB, err := openMySQL(ctx, mysqlDataSource)
	if err != nil {
		return err
	}
	defer mysqlDB.Close()

	clickhouseConn, err := openClickHouse(ctx, clickhouseDataSource)
	if err != nil {
		return err
	}
	defer clickhouseConn.Close()

	clickHouseBoundary, err := resolveClickHouseBoundary(ctx, clickhouseConn)
	if err != nil {
		return err
	}

	groupRanges, err := queryMySQLUsageGroupRanges(ctx, mysqlDB, clickHouseBoundary)
	if err != nil {
		return err
	}
	oldestMySQLPeriod := oldestUsagePeriod(groupRanges)
	if oldestMySQLPeriod.IsZero() {
		log.Infof("No MySQL usage periods before %s; nothing to backfill.", clickHouseBoundary.Format(time.RFC3339))
		return nil
	}

	log.Infof("Backfilling MySQL Usages to ClickHouse RawUsage.")
	log.Infof("Range: [%s, %s), walking backwards month by month.", startOfUTCMonth(oldestMySQLPeriod).Format(time.RFC3339), clickHouseBoundary.Format(time.RFC3339))
	log.Infof("MySQL groups: %d", len(groupRanges))
	log.Infof("BufferID: <mysql region>:redis")
	maxMonthsToRun := 0
	if *dryRun {
		maxMonthsToRun = dryRunMaxMonths
		log.Infof("Dry run: no ClickHouse rows will be inserted, and at most %d months will be checked.", maxMonthsToRun)
	}

	months, stoppedAfterMaxMonths := makeBackfillMonths(groupRanges, oldestMySQLPeriod, clickHouseBoundary, maxMonthsToRun)
	if err := backfillMonths(ctx, mysqlDB, clickhouseConn, clickHouseBoundary, months); err != nil {
		return err
	}
	if stoppedAfterMaxMonths {
		log.Infof("Stopped after %d months.", maxMonthsToRun)
	}

	if *dryRun {
		log.Infof("Dry-run complete.")
	} else {
		log.Infof("Backfill complete.")
	}
	return nil
}

func makeBackfillMonths(groupRanges []usageGroupRange, oldestMySQLPeriod, clickHouseBoundary time.Time, maxMonthsToRun int) ([]backfillMonth, bool) {
	var months []backfillMonth
	oldestMySQLMonth := startOfUTCMonth(oldestMySQLPeriod)
	cursor := clickHouseBoundary
	for {
		if maxMonthsToRun > 0 && len(months) >= maxMonthsToRun {
			return months, true
		}
		monthStart := cursor.AddDate(0, -1, 0)
		if monthStart.Before(oldestMySQLMonth) {
			monthStart = oldestMySQLMonth
		}
		months = append(months, backfillMonth{
			StartTime:   monthStart,
			EndTime:     cursor,
			GroupRanges: groupRangesForMonth(groupRanges, monthStart, cursor),
		})
		if !monthStart.After(oldestMySQLMonth) {
			break
		}
		cursor = monthStart
	}
	return months, false
}

func flagValue(name string) (string, error) {
	value, err := flagutil.GetDereferencedValue[string](name)
	if err != nil {
		return "", fmt.Errorf("read -%s: %w", name, err)
	}
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("-%s is required", name)
	}
	return value, nil
}

func openMySQL(ctx context.Context, dataSource string) (*sql.DB, error) {
	ds, err := db.ParseDatasource(ctx, dataSource, &db.AdvancedConfig{})
	if err != nil {
		return nil, fmt.Errorf("parse database data source: %w", err)
	}
	if ds.DriverName() != "mysql" {
		return nil, fmt.Errorf("-database.data_source must use mysql://, got %q", ds.DriverName())
	}
	dsn, err := ds.DSN()
	if err != nil {
		return nil, fmt.Errorf("build mysql dsn: %w", err)
	}
	sqlDB, err := sql.Open(ds.DriverName(), dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	if _, err := sqlDB.ExecContext(ctx, "SET time_zone = '+00:00'"); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("set mysql time zone: %w", err)
	}
	return sqlDB, nil
}

func openClickHouse(ctx context.Context, dataSource string) (ch.Conn, error) {
	options, err := ch.ParseDSN(dataSource)
	if err != nil {
		return nil, fmt.Errorf("parse olap database data source: %w", err)
	}
	conn, err := ch.Open(options)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	return conn, nil
}

func resolveClickHouseBoundary(ctx context.Context, conn ch.Conn) (time.Time, error) {
	var count uint64
	var minPeriodStart time.Time
	if err := conn.QueryRow(ctx, `
		SELECT count(), min(period_start)
		FROM RawUsage
	`).Scan(&count, &minPeriodStart); err != nil {
		return time.Time{}, fmt.Errorf("query earliest clickhouse usage period: %w", err)
	}
	if count == 0 {
		return startOfUTCMonth(time.Now()).AddDate(0, 1, 0), nil
	}
	return startOfUTCMonth(minPeriodStart), nil
}

func startOfUTCMonth(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

func queryMySQLUsageGroupRanges(ctx context.Context, db *sql.DB, clickHouseBoundary time.Time) ([]usageGroupRange, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			group_id,
			MIN(period_start_usec) AS min_period_start_usec,
			MAX(period_start_usec) AS max_period_start_usec
		FROM Usages FORCE INDEX (group_period_region_index_v2)
		WHERE period_start_usec < ?
		GROUP BY group_id
		ORDER BY group_id
	`, clickHouseBoundary.UnixMicro())
	if err != nil {
		return nil, fmt.Errorf("query mysql usage group ranges: %w", err)
	}
	defer rows.Close()

	var groupRanges []usageGroupRange
	for rows.Next() {
		var groupRange usageGroupRange
		if err := rows.Scan(&groupRange.GroupID, &groupRange.MinPeriodStartUsec, &groupRange.MaxPeriodStartUsec); err != nil {
			return nil, err
		}
		groupRanges = append(groupRanges, groupRange)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return groupRanges, nil
}

func oldestUsagePeriod(groupRanges []usageGroupRange) time.Time {
	if len(groupRanges) == 0 {
		return time.Time{}
	}
	oldest := groupRanges[0].MinPeriodStartUsec
	for _, groupRange := range groupRanges[1:] {
		oldest = min(oldest, groupRange.MinPeriodStartUsec)
	}
	return time.UnixMicro(oldest).UTC()
}

func groupRangesForMonth(groupRanges []usageGroupRange, startTime, endTime time.Time) []usageGroupRange {
	startUsec := startTime.UnixMicro()
	endUsec := endTime.UnixMicro()
	monthGroupRanges := make([]usageGroupRange, 0, len(groupRanges))
	for _, groupRange := range groupRanges {
		if groupRange.MaxPeriodStartUsec < startUsec {
			continue
		}
		if groupRange.MinPeriodStartUsec >= endUsec {
			continue
		}
		monthGroupRanges = append(monthGroupRanges, groupRange)
	}
	return monthGroupRanges
}

func backfillMonths(ctx context.Context, mysqlDB *sql.DB, clickhouseConn ch.Conn, clickHouseBoundary time.Time, months []backfillMonth) error {
	if len(months) == 0 {
		return nil
	}

	yearCache := &usageYearCache{
		clickHouseBoundary: clickHouseBoundary,
		yearValues:         make(map[groupYearKey]bool),
	}
	for _, month := range months {
		if err := backfillOneMonth(ctx, mysqlDB, clickhouseConn, yearCache, month); err != nil {
			return err
		}
	}
	return nil
}

func backfillOneMonth(ctx context.Context, mysqlDB *sql.DB, clickhouseConn ch.Conn, yearCache *usageYearCache, month backfillMonth) error {
	if len(month.GroupRanges) == 0 {
		log.Infof("%s: no groups overlap month", formatMonth(month.StartTime))
		return nil
	}

	eg, ctx := errgroup.WithContext(ctx)
	tasks := make(chan mysqlQueryTask, mysqlQueryWorkers)
	results := make(chan mysqlQueryResult, mysqlQueryWorkers)
	batches := make(chan []schema.RawUsage, clickHouseWriterWorkers)
	monthLabel := formatMonth(month.StartTime)

	eg.Go(func() error {
		defer close(tasks)
		for _, groupRange := range month.GroupRanges {
			select {
			case tasks <- mysqlQueryTask{Month: month, GroupRange: groupRange}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	var mysqlWG sync.WaitGroup
	mysqlWG.Add(mysqlQueryWorkers)
	for range mysqlQueryWorkers {
		eg.Go(func() error {
			defer mysqlWG.Done()
			for task := range tasks {
				result, err := queryRawUsageRowsForGroup(ctx, mysqlDB, yearCache, task)
				if err != nil {
					return err
				}
				select {
				case results <- result:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
	}

	eg.Go(func() error {
		mysqlWG.Wait()
		close(results)
		return nil
	})

	eg.Go(func() error {
		defer close(batches)
		return collectRawUsageBatches(ctx, month, results, batches)
	})

	for range clickHouseWriterWorkers {
		eg.Go(func() error {
			for {
				select {
				case batch, ok := <-batches:
					if !ok {
						return nil
					}
					if *dryRun {
						log.Infof("%s: dry run: would insert ClickHouse batch with %d RawUsage rows.", monthLabel, len(batch))
						continue
					}
					insertStart := time.Now()
					if err := insertRawUsageBatch(ctx, clickhouseConn, batch); err != nil {
						return err
					}
					log.Infof("%s: inserted ClickHouse batch with %d RawUsage rows in %s.", monthLabel, len(batch), time.Since(insertStart).Round(time.Millisecond))
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}

	return eg.Wait()
}

func queryRawUsageRowsForGroup(ctx context.Context, db *sql.DB, yearCache *usageYearCache, task mysqlQueryTask) (mysqlQueryResult, error) {
	hasUsageInYear, err := yearCache.hasUsageForYear(ctx, db, task.GroupRange.GroupID, task.Month.StartTime.Year())
	if err != nil {
		return mysqlQueryResult{}, err
	}
	if !hasUsageInYear {
		return mysqlQueryResult{
			GroupID: task.GroupRange.GroupID,
			Skipped: true,
		}, nil
	}
	mysqlRows, err := queryMySQLUsageForGroup(ctx, db, task.GroupRange.GroupID, task.Month.StartTime, task.Month.EndTime)
	if err != nil {
		return mysqlQueryResult{}, fmt.Errorf("query mysql usage %s group %q: %w", formatMonth(task.Month.StartTime), task.GroupRange.GroupID, err)
	}
	aggregatedRows := aggregateUsageRows(mysqlRows)
	rawRows, err := buildRawUsageRows(aggregatedRows)
	if err != nil {
		return mysqlQueryResult{}, fmt.Errorf("build RawUsage rows %s group %q: %w", formatMonth(task.Month.StartTime), task.GroupRange.GroupID, err)
	}
	return mysqlQueryResult{
		GroupID:      task.GroupRange.GroupID,
		MySQLRows:    len(aggregatedRows),
		RawUsageRows: rawRows,
	}, nil
}

func (c *usageYearCache) hasUsageForYear(ctx context.Context, db *sql.DB, groupID string, year int) (bool, error) {
	key := groupYearKey{GroupID: groupID, Year: year}
	c.mu.Lock()
	if hasUsage, ok := c.yearValues[key]; ok {
		c.mu.Unlock()
		return hasUsage, nil
	}
	c.mu.Unlock()

	value, err, _ := c.group.Do(fmt.Sprintf("year:%s:%d", groupID, year), func() (any, error) {
		c.mu.Lock()
		if hasUsage, ok := c.yearValues[key]; ok {
			c.mu.Unlock()
			return hasUsage, nil
		}
		c.mu.Unlock()

		hasUsage, err := queryMySQLUsageExistsForYear(ctx, db, groupID, year, c.clickHouseBoundary)
		if err != nil {
			return false, fmt.Errorf("query mysql usage existence for group %q year %d: %w", groupID, year, err)
		}

		c.mu.Lock()
		c.yearValues[key] = hasUsage
		c.mu.Unlock()
		return hasUsage, nil
	})
	if err != nil {
		return false, err
	}
	return value.(bool), nil
}

func queryMySQLUsageExistsForYear(ctx context.Context, db *sql.DB, groupID string, year int, clickHouseBoundary time.Time) (bool, error) {
	start := time.Date(year, 1, 1, 0, 0, 0, 0, time.UTC)
	endUsec := time.Date(year+1, 1, 1, 0, 0, 0, 0, time.UTC).UnixMicro()
	if boundaryEndUsec := clickHouseBoundary.UnixMicro(); boundaryEndUsec < endUsec {
		endUsec = boundaryEndUsec
	}
	if endUsec <= start.UnixMicro() {
		return false, nil
	}

	var found int
	if err := db.QueryRowContext(ctx, `
		SELECT 1
		FROM Usages FORCE INDEX (group_period_region_index_v2)
		WHERE group_id = ?
			AND period_start_usec >= ?
			AND period_start_usec < ?
		LIMIT 1
	`, groupID, start.UnixMicro(), endUsec).Scan(&found); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func collectRawUsageBatches(ctx context.Context, month backfillMonth, results <-chan mysqlQueryResult, batches chan<- []schema.RawUsage) error {
	batch := make([]schema.RawUsage, 0, *insertBatchSize)
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		batchCopy := append([]schema.RawUsage(nil), batch...)
		batch = batch[:0]
		select {
		case batches <- batchCopy:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	for result := range results {
		if result.Skipped {
			continue
		}
		logGroupMonthResult(month, result)
		rows := result.RawUsageRows
		for len(rows) > 0 {
			n := min(cap(batch)-len(batch), len(rows))
			batch = append(batch, rows[:n]...)
			rows = rows[n:]
			if len(batch) == cap(batch) {
				if err := flushBatch(); err != nil {
					return err
				}
			}
		}
	}
	return flushBatch()
}

func logGroupMonthResult(month backfillMonth, result mysqlQueryResult) {
	monthLabel := formatMonth(month.StartTime)
	if result.MySQLRows == 0 {
		log.Infof("%s group=%s: no MySQL usage rows", monthLabel, result.GroupID)
		return
	}
	if len(result.RawUsageRows) == 0 {
		log.Infof("%s group=%s: MySQL usage rows=%d, no positive RawUsage counts", monthLabel, result.GroupID, result.MySQLRows)
		return
	}
	log.Infof("%s group=%s: MySQL usage rows=%d, RawUsage rows=%d", monthLabel, result.GroupID, result.MySQLRows, len(result.RawUsageRows))
}

func formatMonth(startTime time.Time) string {
	return fmt.Sprintf("month=%s", startTime.Format("2006-01"))
}

func aggregateUsageRows(rows []tables.Usage) []tables.Usage {
	aggregateByKey := make(map[usageAggregateKey]*tables.Usage)
	for _, row := range rows {
		key := usageKey(row)
		existing := aggregateByKey[key]
		if existing == nil {
			rowCopy := row
			aggregateByKey[key] = &rowCopy
			continue
		}
		addUsageCounts(&existing.UsageCounts, row.UsageCounts)
	}

	result := make([]tables.Usage, 0, len(aggregateByKey))
	for _, row := range aggregateByKey {
		result = append(result, *row)
	}
	sort.Slice(result, func(i, j int) bool {
		left := usageKey(result[i])
		right := usageKey(result[j])
		if left.PeriodStartUsec != right.PeriodStartUsec {
			return left.PeriodStartUsec > right.PeriodStartUsec
		}
		if left.GroupID != right.GroupID {
			return left.GroupID < right.GroupID
		}
		if left.Region != right.Region {
			return left.Region < right.Region
		}
		if left.Origin != right.Origin {
			return left.Origin < right.Origin
		}
		if left.Client != right.Client {
			return left.Client < right.Client
		}
		return left.Server < right.Server
	})
	return result
}

func queryMySQLUsageForGroup(ctx context.Context, db *sql.DB, groupID string, startTime, endTime time.Time) ([]tables.Usage, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			period_start_usec,
			group_id,
			COALESCE(region, '') AS region,
			COALESCE(origin, '') AS origin,
			COALESCE(client, '') AS client,
			COALESCE(server, '') AS server,
			invocations,
			cas_cache_hits,
			action_cache_hits,
			total_download_size_bytes,
			linux_execution_duration_usec,
			mac_execution_duration_usec,
			self_hosted_linux_execution_duration_usec,
			self_hosted_mac_execution_duration_usec,
			total_upload_size_bytes,
			total_cached_action_exec_usec,
			cpu_nanos,
			memory_gb_usec,
			linux_arm64_execution_burstable_compute_duration_usec,
			linux_arm64_execution_compute_duration_usec,
			linux_x86_64_execution_burstable_compute_duration_usec,
			linux_x86_64_execution_compute_duration_usec,
			darwin_arm64_execution_burstable_compute_duration_usec,
			darwin_arm64_execution_compute_duration_usec,
			darwin_x86_64_execution_burstable_compute_duration_usec,
			darwin_x86_64_execution_compute_duration_usec
		FROM Usages FORCE INDEX (group_period_region_index_v2)
		WHERE group_id = ?
			AND period_start_usec >= ?
			AND period_start_usec < ?
	`, groupID, startTime.UnixMicro(), endTime.UnixMicro())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []tables.Usage
	for rows.Next() {
		var row tables.Usage
		if err := rows.Scan(
			&row.PeriodStartUsec,
			&row.GroupID,
			&row.Region,
			&row.UsageLabels.Origin,
			&row.UsageLabels.Client,
			&row.UsageLabels.Server,
			&row.UsageCounts.Invocations,
			&row.UsageCounts.CASCacheHits,
			&row.UsageCounts.ActionCacheHits,
			&row.UsageCounts.TotalDownloadSizeBytes,
			&row.UsageCounts.LinuxExecutionDurationUsec,
			&row.UsageCounts.MacExecutionDurationUsec,
			&row.UsageCounts.SelfHostedLinuxExecutionDurationUsec,
			&row.UsageCounts.SelfHostedMacExecutionDurationUsec,
			&row.UsageCounts.TotalUploadSizeBytes,
			&row.UsageCounts.TotalCachedActionExecUsec,
			&row.UsageCounts.CPUNanos,
			&row.UsageCounts.MemoryGBUsec,
			&row.UsageCounts.LinuxArm64ExecutionBurstableComputeDurationUsec,
			&row.UsageCounts.LinuxArm64ExecutionComputeDurationUsec,
			&row.UsageCounts.LinuxX86_64ExecutionBurstableComputeDurationUsec,
			&row.UsageCounts.LinuxX86_64ExecutionComputeDurationUsec,
			&row.UsageCounts.DarwinArm64ExecutionBurstableComputeDurationUsec,
			&row.UsageCounts.DarwinArm64ExecutionComputeDurationUsec,
			&row.UsageCounts.DarwinX86_64ExecutionBurstableComputeDurationUsec,
			&row.UsageCounts.DarwinX86_64ExecutionComputeDurationUsec,
		); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func usageKey(row tables.Usage) usageAggregateKey {
	return usageAggregateKey{
		PeriodStartUsec: row.PeriodStartUsec,
		GroupID:         row.GroupID,
		Region:          row.Region,
		Origin:          row.UsageLabels.Origin,
		Client:          row.UsageLabels.Client,
		Server:          row.UsageLabels.Server,
	}
}

func addUsageCounts(dst *tables.UsageCounts, src tables.UsageCounts) {
	dst.Invocations += src.Invocations
	dst.CASCacheHits += src.CASCacheHits
	dst.ActionCacheHits += src.ActionCacheHits
	dst.TotalDownloadSizeBytes += src.TotalDownloadSizeBytes
	dst.LinuxExecutionDurationUsec += src.LinuxExecutionDurationUsec
	dst.MacExecutionDurationUsec += src.MacExecutionDurationUsec
	dst.SelfHostedLinuxExecutionDurationUsec += src.SelfHostedLinuxExecutionDurationUsec
	dst.SelfHostedMacExecutionDurationUsec += src.SelfHostedMacExecutionDurationUsec
	dst.TotalUploadSizeBytes += src.TotalUploadSizeBytes
	dst.TotalCachedActionExecUsec += src.TotalCachedActionExecUsec
	dst.CPUNanos += src.CPUNanos
	dst.MemoryGBUsec += src.MemoryGBUsec
	dst.LinuxArm64ExecutionBurstableComputeDurationUsec += src.LinuxArm64ExecutionBurstableComputeDurationUsec
	dst.LinuxArm64ExecutionComputeDurationUsec += src.LinuxArm64ExecutionComputeDurationUsec
	dst.LinuxX86_64ExecutionBurstableComputeDurationUsec += src.LinuxX86_64ExecutionBurstableComputeDurationUsec
	dst.LinuxX86_64ExecutionComputeDurationUsec += src.LinuxX86_64ExecutionComputeDurationUsec
	dst.DarwinArm64ExecutionBurstableComputeDurationUsec += src.DarwinArm64ExecutionBurstableComputeDurationUsec
	dst.DarwinArm64ExecutionComputeDurationUsec += src.DarwinArm64ExecutionComputeDurationUsec
	dst.DarwinX86_64ExecutionBurstableComputeDurationUsec += src.DarwinX86_64ExecutionBurstableComputeDurationUsec
	dst.DarwinX86_64ExecutionComputeDurationUsec += src.DarwinX86_64ExecutionComputeDurationUsec
}

func buildRawUsageRows(rows []tables.Usage) ([]schema.RawUsage, error) {
	var result []schema.RawUsage
	for _, row := range rows {
		baseLabels := makeBaseLabels(row)
		periodStart := time.UnixMicro(row.PeriodStartUsec).UTC()
		region := strings.TrimSpace(row.Region)
		if region == "" {
			return nil, errors.New("mysql usage row has empty region")
		}
		bufferID := region + ":redis"

		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.BuildEventsBESCount, baseLabels, row.UsageCounts.Invocations, 1, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteCacheCASHits, baseLabels, row.UsageCounts.CASCacheHits, 1, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteCacheACHits, baseLabels, row.UsageCounts.ActionCacheHits, 1, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteCacheCASDownloadedBytes, baseLabels, row.UsageCounts.TotalDownloadSizeBytes, 1, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteCacheCASUploadedBytes, baseLabels, row.UsageCounts.TotalUploadSizeBytes, 1, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteCacheACCachedExecDurationNanos, baseLabels, row.UsageCounts.TotalCachedActionExecUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteWorkerDurationNanos, withExecutionLabels(baseLabels, sku.OSLinux, sku.SelfHostedFalse), row.UsageCounts.LinuxExecutionDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteWorkerDurationNanos, withExecutionLabels(baseLabels, sku.OSMac, sku.SelfHostedFalse), row.UsageCounts.MacExecutionDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteWorkerDurationNanos, withExecutionLabels(baseLabels, sku.OSLinux, sku.SelfHostedTrue), row.UsageCounts.SelfHostedLinuxExecutionDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteWorkerDurationNanos, withExecutionLabels(baseLabels, sku.OSMac, sku.SelfHostedTrue), row.UsageCounts.SelfHostedMacExecutionDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteWorkerCPUNanos, withExecutionLabels(baseLabels, sku.OSLinux, sku.SelfHostedFalse), row.UsageCounts.CPUNanos, 1, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteWorkerMemoryGBNanos, withExecutionLabels(baseLabels, sku.OSLinux, sku.SelfHostedFalse), row.UsageCounts.MemoryGBUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteFlexibleComputeNanos, withComputeLabels(baseLabels, sku.OSLinux, sku.ArchArm64), row.UsageCounts.LinuxArm64ExecutionBurstableComputeDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteFixedComputeNanos, withComputeLabels(baseLabels, sku.OSLinux, sku.ArchArm64), row.UsageCounts.LinuxArm64ExecutionComputeDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteFlexibleComputeNanos, withComputeLabels(baseLabels, sku.OSLinux, sku.ArchX86_64), row.UsageCounts.LinuxX86_64ExecutionBurstableComputeDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteFixedComputeNanos, withComputeLabels(baseLabels, sku.OSLinux, sku.ArchX86_64), row.UsageCounts.LinuxX86_64ExecutionComputeDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteFlexibleComputeNanos, withComputeLabels(baseLabels, sku.OSMac, sku.ArchArm64), row.UsageCounts.DarwinArm64ExecutionBurstableComputeDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteFixedComputeNanos, withComputeLabels(baseLabels, sku.OSMac, sku.ArchArm64), row.UsageCounts.DarwinArm64ExecutionComputeDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteFlexibleComputeNanos, withComputeLabels(baseLabels, sku.OSMac, sku.ArchX86_64), row.UsageCounts.DarwinX86_64ExecutionBurstableComputeDurationUsec, 1_000, bufferID)
		result = appendRawUsageRows(result, row.GroupID, periodStart, sku.RemoteExecutionExecuteFixedComputeNanos, withComputeLabels(baseLabels, sku.OSMac, sku.ArchX86_64), row.UsageCounts.DarwinX86_64ExecutionComputeDurationUsec, 1_000, bufferID)
	}
	return result, nil
}

func appendRawUsageRows(rows []schema.RawUsage, groupID string, periodStart time.Time, usageSKU sku.SKU, labels map[string]string, count, scale int64, bufferID string) []schema.RawUsage {
	if count <= 0 {
		return rows
	}
	count *= scale
	return append(rows, schema.RawUsage{
		GroupID:     groupID,
		SKU:         usageSKU,
		Labels:      orderedmap.FromMap(maps.Clone(labels)),
		PeriodStart: periodStart,
		BufferID:    bufferID,
		Count:       count,
	})
}

func makeBaseLabels(row tables.Usage) map[string]string {
	labels := make(map[string]string, 3)
	if row.UsageLabels.Origin != "" {
		labels[sku.Origin] = row.UsageLabels.Origin
	}
	if row.UsageLabels.Client != "" {
		labels[sku.Client] = row.UsageLabels.Client
	}
	if row.UsageLabels.Server != "" {
		labels[sku.Server] = row.UsageLabels.Server
	}
	return labels
}

func withExecutionLabels(base map[string]string, osLabel, selfHosted string) map[string]string {
	labels := maps.Clone(base)
	labels[sku.OS] = osLabel
	labels[sku.SelfHosted] = selfHosted
	return labels
}

func withComputeLabels(base map[string]string, osLabel, archLabel string) map[string]string {
	labels := withExecutionLabels(base, osLabel, sku.SelfHostedFalse)
	labels[sku.Arch] = archLabel
	return labels
}

func insertRawUsageBatch(ctx context.Context, conn ch.Conn, rows []schema.RawUsage) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO RawUsage (group_id, sku, labels, period_start, buffer_id, count)")
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}
	for _, row := range rows {
		if err := batch.Append(row.GroupID, row.SKU, row.Labels, row.PeriodStart, row.BufferID, row.Count); err != nil {
			return fmt.Errorf("append row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}
	return nil
}

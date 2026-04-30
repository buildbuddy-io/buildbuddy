// This script can be used to generate load on a ClickHouse test database.
//
// First, export sample data from the ClickHouse database:
//
// kubectl port-forward ... # port-forward to ClickHouse
// bazel run //enterprise/tools/clickhouse_loadgen -- --olap_database.data_source=clickhouse://localhost:9001/buildbuddy_prod --export --export_out=/tmp/sample_data_DATE.out
// gsutil cp /tmp/sample_data_DATE.out gs://BUCKET/sample_data_DATE.out
//
// Then, generate load (should run this as a scraped pod on k8s so you can view metrics).
// Make sure to set the flags - see the flags below.
// clickhouse_loadgen [ FLAGS ... ]

package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/monitoring"
	"github.com/buildbuddy-io/buildbuddy/server/util/qps"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
)

var (
	// These flags should be overridden when running for real:
	sampleStartUsec = flag.Int64("sample_start_usec", time.Now().Add(-24*time.Hour).UnixMicro(), "Sample data start timestamp. Typically set to the start of a recent weekday.")
	sampleEndUsec   = flag.Int64("sample_end_usec", 0, "Sample data end timestamp. If 0, set to sample_start_usec + 24 hours.")
	readonly        = flag.Bool("readonly", true, "If true, don't generate write queries.")

	// Trends QPS is typically very small but can spike up to 0.5/s when looking
	// at the max 1-minute bucket:
	// SELECT toStartOfMinute(event_time) AS minute, COUNT(*) / 60 AS qps
	// FROM system.query_log
	// WHERE query LIKE 'SELECT%bucket_start_time_micros%' AND event_time >= addDays(now(), -7)
	// GROUP BY minute
	// ORDER BY qps DESC LIMIT 1;
	trendsQueryRate = flag.Float64("trends_qps", 0.5, "How many invocation trends queries to run per second.")
	// Determine using rate of execution insert queries in "ClickHouse" panel of Grafana chart.
	executionInsertRate = flag.Float64("execution_insert_qps", 1.0, "How many execution insert queries to run per second.")

	monitoringAddress = flag.String("monitoring_address", "0.0.0.0:9090", "Address to run monitoring server on.")
)

const (
	// How many of each type of query that can be run concurrently. If queries
	// are too slow then this limit may eventually be hit, and workers will not
	// hit their target QPS.
	maxConcurrentQueries = 10
)

func main() {
	flag.Parse()
	if err := run(); err != nil {
		log.Fatal(err.Error())
	}
}

func run() error {
	if err := log.Configure(); err != nil {
		return fmt.Errorf("configure logging: %w", err)
	}
	if err := flagutil.SetValueForFlagName("olap_database.auto_migrate_db", false, nil, false); err != nil {
		return fmt.Errorf("disable auto-migration: %w", err)
	}
	if *sampleEndUsec == 0 {
		*sampleEndUsec = *sampleStartUsec + 24*time.Hour.Microseconds()
	}

	if err := configsecrets.Configure(); err != nil {
		return fmt.Errorf("set up config secrets: %w", err)
	}
	if err := config.Load(); err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	hc := healthcheck.NewHealthChecker("clickhouse-loadgen")
	env := real_environment.NewRealEnv(hc)

	ctx, cancel := context.WithCancel(env.GetServerContext())
	defer cancel()
	eg, ctx := errgroup.WithContext(ctx)
	hc.RegisterShutdownFunction(func(_ context.Context) error {
		cancel()
		return eg.Wait()
	})

	if err := clickhouse.Register(env); err != nil {
		return fmt.Errorf("register clickhouse: %w", err)
	}
	if env.GetOLAPDBHandle() == nil {
		return fmt.Errorf("no OLAP DB handle registered: missing --olap_database.data_source flag?")
	}

	monitoring.StartMonitoringHandler(env, *monitoringAddress)

	sampleData, err := querySampleData(ctx, env)
	if err != nil {
		return fmt.Errorf("query sample data: %w", err)
	}

	eg.Go(func() error {
		return generateLoad(ctx, env, sampleData, "get_trends", *trendsQueryRate, trendsQuery)
	})
	if !*readonly {
		eg.Go(func() error {
			return generateLoad(ctx, env, sampleData, "insert_executions", *executionInsertRate, executionsInsertQuery)
		})
	}

	hc.WaitForGracefulShutdown()
	return nil
}

type QueryFn func(ctx context.Context, env environment.Env, sampleData *SampleData) error

func generateLoad(ctx context.Context, env environment.Env, sampleData *SampleData, name string, qpsTarget float64, queryFn QueryFn) error {
	log.Infof("Starting load generator %q with target QPS %.3f/s", name, qpsTarget)
	qps := qps.NewCounter(30*time.Second, env.GetClock())
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Infof("%s query QPS: %.3f/s", name, qps.Get())
			}
		}
	}()
	r := rate.NewLimiter(rate.Limit(qpsTarget), 1)
	var eg errgroup.Group
	eg.SetLimit(maxConcurrentQueries)
	for {
		if err := r.Wait(ctx); err != nil {
			return err
		}
		eg.Go(func() error {
			if err := queryFn(ctx, env, sampleData); err != nil {
				log.Warningf("Failed to run %s query: %s", name, err)
			} else {
				qps.Inc()
			}
			return nil
		})
	}
}

func trendsQuery(ctx context.Context, env environment.Env, sampleData *SampleData) error {
	// Get example trends query using:
	/*
		SELECT query
		FROM system.query_log
		-- update based on invocation_stats_service.go:getTrendBasicQuery
		WHERE query LIKE 'SELECT%bucket_start_time_micros%Invocations%'
		ORDER BY event_time DESC
		LIMIT 1;
	*/
	_, err := db.ScanAll(env.GetOLAPDBHandle().NewQuery(ctx, "loadgen_trends").Raw(`
		SELECT bucket_start_time_micros,
		total_num_builds,
		successful_builds,
		failed_builds,
		other_builds,
		total_build_time_usec,
		completed_invocation_count,
		user_count,
		commit_count,
		host_count,
		repo_count,
		branch_count,
		max_duration_usec,
		action_cache_hits,
		action_cache_misses,
		action_cache_uploads,
		cas_cache_hits,
		cas_cache_misses,
		cas_cache_uploads,
		total_download_size_bytes,
		total_upload_size_bytes,
		total_download_usec,
		total_upload_usec,
		total_cpu_micros_saved,
		arrayElement(build_time_quantiles, 1) as build_time_usec_p50,
		arrayElement(build_time_quantiles, 2) as build_time_usec_p75,
		arrayElement(build_time_quantiles, 3) as build_time_usec_p90,
		arrayElement(build_time_quantiles, 4) as build_time_usec_p95,
		arrayElement(build_time_quantiles, 5) as build_time_usec_p99
		FROM (
		SELECT toUnixTimestamp(toStartOfInterval(toDateTime64(updated_at_usec / 1000000, 6, 'America/Los_Angeles'), INTERVAL 4 HOUR)) * 1000000 as bucket_start_time_micros,
			quantilesExactExclusive(0.5, 0.75, 0.9, 0.95, 0.99)(IF(duration_usec > 0, duration_usec, 0)) AS build_time_quantiles,
			COUNT(1) AS total_num_builds,
			SUM(CASE WHEN success AND invocation_status = 1 THEN 1 ELSE 0 END) as successful_builds,
			SUM(CASE WHEN NOT success AND invocation_status = 1 THEN 1 ELSE 0 END) as failed_builds,
			SUM(CASE WHEN invocation_status <> 1 THEN 1 ELSE 0 END) as other_builds,
			SUM(CASE WHEN duration_usec > 0 THEN duration_usec END) as total_build_time_usec,
			SUM(CASE WHEN duration_usec > 0 THEN 1 ELSE 0 END) as completed_invocation_count,
			COUNT(DISTINCT "user") as user_count,
			COUNT(DISTINCT commit_sha) as commit_count,
			COUNT(DISTINCT host) as host_count,
			COUNT(DISTINCT repo_url) as repo_count,
			COUNT(DISTINCT branch_name) as branch_count,
			MAX(duration_usec) as max_duration_usec,
			SUM(action_cache_hits) as action_cache_hits,
			SUM(action_cache_misses) as action_cache_misses,
			SUM(action_cache_uploads) as action_cache_uploads,
			SUM(cas_cache_hits) as cas_cache_hits,
			SUM(cas_cache_misses) as cas_cache_misses,
			SUM(cas_cache_uploads) as cas_cache_uploads,
			SUM(total_download_size_bytes) as total_download_size_bytes,
			SUM(total_upload_size_bytes) as total_upload_size_bytes,
			SUM(total_download_usec) as total_download_usec,
			SUM(total_upload_usec) as total_upload_usec,
			SUM(total_cached_action_exec_usec) as total_cpu_micros_saved
			FROM "Invocations"
			WHERE ( updated_at_usec >= ? )
			AND ( updated_at_usec <= ? )
			AND ( group_id = ? )
			GROUP BY  bucket_start_time_micros
		)
	`, *sampleStartUsec, *sampleEndUsec, sampleData.InvocationQueryGroupID), &struct{}{})
	return err
}

var uuidRegexp = regexp.MustCompile(`[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}`)

func executionsInsertQuery(ctx context.Context, env environment.Env, sampleData *SampleData) error {
	// Pick random execution batch (weighted), modify unique IDs, then
	// insert the batch.
	w := rand.Float64()
	batch := sampleData.SampleExecutionBatches[len(sampleData.SampleExecutionBatches)-1]
	for _, b := range sampleData.SampleExecutionBatches {
		w -= b.Quantile
		if w <= 0 {
			break
		}
	}
	rowsToInsert := slices.Clone(batch.ExecutionRows)
	for _, row := range rowsToInsert {
		row.InvocationUUID = strings.ReplaceAll(uuid.New(), "-", "")
		row.ExecutionID = uuidRegexp.ReplaceAllString(row.ExecutionID, uuid.New())
		row.UpdatedAtUsec = time.Now().UnixMicro()
	}
	return env.GetOLAPDBHandle().NewQuery(ctx, "loadgen_execution_insert").Create(batch.ExecutionRows)
}

type SampleData struct {
	// Group ID to use for testing invocation table queries.
	InvocationQueryGroupID string
	// Execution batches to use for testing execution insert queries.
	SampleExecutionBatches []SampleExecutionBatch
}

type SampleExecutionBatch struct {
	Quantile      float64
	ExecutionRows []*schema.Execution
}

func querySampleData(ctx context.Context, env environment.Env) (*SampleData, error) {
	// Executions (for INSERT load). Executions has >10x more rows than the
	// other tables, so just do executions for now.
	type invocationExecutionCount struct {
		GroupID          string
		InvocationUUID   string
		MinUpdatedAtUsec int64
		MaxUpdatedAtUsec int64
		Count            int64
	}
	log.Infof("Getting invocation execution counts")
	invStatRows, err := db.ScanAll(env.GetOLAPDBHandle().NewQuery(ctx, "get_invocation_execution_counts").Raw(`
		SELECT
			group_id,
			invocation_uuid,
			min(updated_at_usec) AS min_updated_at_usec,
			max(updated_at_usec) AS max_updated_at_usec,
			COUNT(*) AS count
		FROM Executions
		WHERE
			updated_at_usec >= ?
			AND updated_at_usec <= ?
		GROUP BY group_id, invocation_uuid
		ORDER BY count ASC
		LIMIT 1000000
	`, *sampleStartUsec, *sampleEndUsec), &invocationExecutionCount{})
	if err != nil {
		return nil, fmt.Errorf("scan executions: %w", err)
	}

	log.Infof("Smallest execution batch: %d", invStatRows[0].Count)
	log.Infof("Largest execution batch: %d", invStatRows[len(invStatRows)-1].Count)

	// Find representative Execution batches for various quantiles. This
	// approach ensures generates reasonably consistent load patterns (which
	// allows easier comparison before/after changes) while being somewhat
	// realistic.
	var sampleData SampleData
	for _, q := range []float64{0.001, 0.010, 0.05, 0.10, 0.25, 0.5, 0.75, 0.9, 0.95, 0.99, 0.999} {
		idx := int(float64(len(invStatRows)) * q)
		log.Infof("Querying executions for invocation with q%.3f execution insert batch size (%d executions)", q, invStatRows[idx].Count)

		// Query executions for the invocation UUID
		executions, err := db.ScanAll(env.GetOLAPDBHandle().NewQuery(ctx, "export_executions").Raw(`
			SELECT * FROM Executions
			WHERE
				group_id = ?
				AND invocation_uuid = ?
				AND updated_at_usec >= ?
				AND updated_at_usec <= ?
			ORDER BY updated_at_usec ASC
			LIMIT ?
			`,
			invStatRows[idx].GroupID,
			invStatRows[idx].InvocationUUID,
			invStatRows[idx].MinUpdatedAtUsec,
			invStatRows[idx].MaxUpdatedAtUsec,
			invStatRows[idx].Count,
		), &schema.Execution{})
		if err != nil {
			return nil, fmt.Errorf("query executions for invocation %q: %w", invStatRows[idx].InvocationUUID, err)
		}
		sampleData.SampleExecutionBatches = append(
			sampleData.SampleExecutionBatches, SampleExecutionBatch{
				Quantile:      q,
				ExecutionRows: executions,
			},
		)
	}

	// Find the group with the highest invocation creation rate (hourly).
	// Then sample invocations from that group. These invocations will be
	// used to generate invocations and run stats/trends queries on them.
	type groupInvocationCount struct {
		GroupID string
	}
	var res groupInvocationCount
	log.Infof("Finding group with highest invocation creation rate")
	err = env.GetOLAPDBHandle().NewQuery(ctx, "get_group_with_highest_invocation_creation_rate").Raw(`
		SELECT
			group_id,
			toStartOfHour(fromUnixTimestamp64Micro(updated_at_usec)) AS hour,
			COUNT(*) AS count
		FROM Invocations
		WHERE updated_at_usec >= ?
		GROUP BY group_id, hour
		ORDER BY count DESC
		LIMIT 1
	`, *sampleStartUsec, *sampleEndUsec).Take(&res)
	if err != nil {
		return nil, fmt.Errorf("get group with highest invocation creation rate: %w", err)
	}

	sampleData.InvocationQueryGroupID = res.GroupID

	return &sampleData, nil
}

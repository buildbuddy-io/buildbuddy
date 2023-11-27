package invocation_stat_service

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/invocation_stat_service/config"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	sfpb "github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	stpb "github.com/buildbuddy-io/buildbuddy/proto/stats"
)

var (
	readFromOLAPDBEnabled          = flag.Bool("app.enable_read_from_olap_db", true, "If enabled, read from OLAP DB")
	executionTrendsEnabled         = flag.Bool("app.enable_execution_trends", true, "If enabled, fill execution trend stats in GetTrendResponse")
	invocationPercentilesEnabled   = flag.Bool("app.enable_invocation_stat_percentiles", true, "If enabled, provide percentile breakdowns for invocation stats in GetTrendResponse")
	useTimezoneInHeatmapQueries    = flag.Bool("app.use_timezone_in_heatmap_queries", true, "If enabled, use timezone instead of 'timezone offset' to compute day boundaries in heatmap queries.")
	invocationSummaryAvailableUsec = flag.Int64("app.invocation_summary_available_usec", 0, "The timstamp when the invocation summary is available in the DB")
	tagsInDrilldowns               = flag.Bool("app.fetch_tags_drilldown_data", true, "If enabled, DrilldownType_TAG_DRILLDOWN_TYPE can be returned in GetStatDrilldownRequests")
	finerTimeBuckets               = flag.Bool("app.finer_time_buckets", false, "If enabled, split trends and drilldowns into smaller time buckets when the user has a smaller date range selected.")
)

type InvocationStatService struct {
	env     environment.Env
	dbh     interfaces.DBHandle
	olapdbh interfaces.OLAPDBHandle
}

func NewInvocationStatService(env environment.Env, dbh interfaces.DBHandle, olapdbh interfaces.OLAPDBHandle) *InvocationStatService {
	return &InvocationStatService{
		env:     env,
		dbh:     dbh,
		olapdbh: olapdbh,
	}
}

func (i *InvocationStatService) getAggColumn(reqCtx *ctxpb.RequestContext, aggType inpb.AggType) (string, error) {
	switch aggType {
	case inpb.AggType_USER_AGGREGATION_TYPE:
		return "user", nil
	case inpb.AggType_HOSTNAME_AGGREGATION_TYPE:
		return "host", nil
	case inpb.AggType_GROUP_ID_AGGREGATION_TYPE:
		return "group_id", nil
	case inpb.AggType_REPO_URL_AGGREGATION_TYPE:
		return "repo_url", nil
	case inpb.AggType_COMMIT_SHA_AGGREGATION_TYPE:
		return "commit_sha", nil
	case inpb.AggType_DATE_AGGREGATION_TYPE:
		// TODO(jdhollen): Nobody is using this and we should probably just remove it.
		return i.dbh.DateFromUsecTimestamp("updated_at_usec", reqCtx.GetTimezoneOffsetMinutes()), nil
	case inpb.AggType_BRANCH_AGGREGATION_TYPE:
		return "branch_name", nil
	case inpb.AggType_PATTERN_AGGREGATION_TYPE:
		return "pattern", nil
	default:
		return "", status.InvalidArgumentErrorf("Unknown or unsupported aggregation column type: %s", aggType)
	}
}

type StatInterval int

const (
	StatInterval5Minutes StatInterval = iota
	StatInterval15Minutes
	StatInterval30Minutes
	StatInterval1Hour
	StatInterval2Hours
	StatInterval4Hours
	StatInterval1Day
)

func (s StatInterval) Duration() time.Duration {
	switch s {
	case StatInterval5Minutes:
		return 5 * time.Minute
	case StatInterval15Minutes:
		return 15 * time.Minute
	case StatInterval30Minutes:
		return 30 * time.Minute
	case StatInterval1Hour:
		return 1 * time.Hour
	case StatInterval2Hours:
		return 2 * time.Hour
	case StatInterval4Hours:
		return 4 * time.Hour
	case StatInterval1Day:
		return 24 * time.Hour
	}
	return 24 * time.Hour
}

func (s StatInterval) IntervalProto() *stpb.StatsInterval {
	switch s {
	case StatInterval5Minutes:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_MINUTE,
			Count: 5,
		}
	case StatInterval15Minutes:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_MINUTE,
			Count: 15,
		}
	case StatInterval30Minutes:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_MINUTE,
			Count: 30,
		}
	case StatInterval1Hour:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_HOUR,
			Count: 1,
		}
	case StatInterval2Hours:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_HOUR,
			Count: 2,
		}
	case StatInterval4Hours:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_HOUR,
			Count: 4,
		}
	case StatInterval1Day:
		return &stpb.StatsInterval{
			Type:  stpb.IntervalType_INTERVAL_TYPE_DAY,
			Count: 1,
		}
	}
	return &stpb.StatsInterval{
		Type:  stpb.IntervalType_INTERVAL_TYPE_DAY,
		Count: 1,
	}
}

func (s StatInterval) ClickhouseInterval() string {
	switch s {
	case StatInterval5Minutes:
		return "5 MINUTE"
	case StatInterval15Minutes:
		return "15 MINUTE"
	case StatInterval30Minutes:
		return "30 MINUTE"
	case StatInterval1Hour:
		return "1 HOUR"
	case StatInterval2Hours:
		return "2 HOUR"
	case StatInterval4Hours:
		return "4 HOUR"
	case StatInterval1Day:
		return "1 DAY"
	}
	return "1 DAY"
}

type trendTimeSettings struct {
	start    *time.Time
	end      *time.Time
	interval StatInterval
	location *time.Location
}

// These values are currently set to keep us under ~50 intervals in a response.
// We need to make some visual improvements to cache charts so that they're
// easier to read with lots of small intervals before we can do more than this.
func computeTrendsInterval(d time.Duration) StatInterval {
	if d <= 3*time.Hour {
		return StatInterval5Minutes
	}
	if d <= 12*time.Hour {
		return StatInterval15Minutes
	}
	// 25 hours so that even when crossing DST, we still show 30-min intervals.
	if d <= 25*time.Hour {
		return StatInterval30Minutes
	}
	if d <= 48*time.Hour {
		return StatInterval1Hour
	}
	if d <= 4*24*time.Hour {
		return StatInterval2Hours
	}
	if d <= 8*24*time.Hour {
		return StatInterval4Hours
	}
	return StatInterval1Day
}

func (i *InvocationStatService) getTrendTimeSettings(tq *stpb.TrendQuery, timezone string) *trendTimeSettings {
	endTime := time.Now()
	if end := tq.GetUpdatedBefore(); end.IsValid() {
		endTime = end.AsTime()
	}
	startTime := endTime.Add(-ONE_WEEK)
	if start := tq.GetUpdatedAfter(); start.IsValid() {
		startTime = start.AsTime()
	}

	location, err := time.LoadLocation(timezone)
	if err != nil || location.String() == time.Local.String() {
		location = time.UTC
	}

	var interval StatInterval
	if !i.finerTimeBucketsEnabled() {
		interval = StatInterval1Day
	} else {
		interval = computeTrendsInterval(endTime.Sub(startTime))
	}

	return &trendTimeSettings{
		start:    &startTime,
		end:      &endTime,
		interval: interval,
		location: location,
	}
}

func (i *InvocationStatService) getTrendBasicQuery(tq *stpb.TrendQuery, timeSettings *trendTimeSettings, timezoneOffsetMinutes int32) (string, []interface{}) {
	var q string
	var qArgs []interface{}
	if i.isOLAPDBEnabled() {
		if i.finerTimeBucketsEnabled() {
			bucketStr, bucketArgs := i.olapdbh.BucketFromUsecTimestamp("updated_at_usec", timeSettings.location, timeSettings.interval.ClickhouseInterval())
			q = fmt.Sprintf("SELECT %s as bucket_start_time_micros,", bucketStr)
			qArgs = bucketArgs
		} else {
			q = fmt.Sprintf("SELECT %s as name,", i.olapdbh.DateFromUsecTimestamp("updated_at_usec", timezoneOffsetMinutes))
		}
	} else {
		q = fmt.Sprintf("SELECT %s as name,", i.dbh.DateFromUsecTimestamp("updated_at_usec", timezoneOffsetMinutes))
	}

	// Insert quantiles stuff..
	if i.isInvocationPercentilesEnabled() {
		q = q + `quantilesExactExclusive(0.5, 0.75, 0.9, 0.95, 0.99)(
				IF(duration_usec > 0, duration_usec, 0)) AS build_time_quantiles,`
	}

	q = q + `
	    COUNT(1) AS total_num_builds,
	    SUM(CASE WHEN duration_usec > 0 THEN duration_usec END) as total_build_time_usec,
	    SUM(CASE WHEN duration_usec > 0 THEN 1 ELSE 0 END) as completed_invocation_count,
	    COUNT(DISTINCT user) as user_count,
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
	    FROM "Invocations"`
	return q, qArgs
}

func (i *InvocationStatService) flattenTrendsQuery(innerQuery string) string {
	var q string
	if i.finerTimeBucketsEnabled() {
		q = "SELECT bucket_start_time_micros,"
	} else {
		q = "SELECT name,"
	}
	return q + `
	total_num_builds,
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
	FROM (` + innerQuery + ")"
}

func (i *InvocationStatService) addWhereClauses(q *query_builder.Query, tq *stpb.TrendQuery, reqCtx *ctxpb.RequestContext) error {

	if user := tq.GetUser(); user != "" {
		q.AddWhereClause("user = ?", user)
	}

	if host := tq.GetHost(); host != "" {
		q.AddWhereClause("host = ?", host)
	}

	if repoURL := tq.GetRepoUrl(); repoURL != "" {
		q.AddWhereClause("repo_url = ?", repoURL)
	}

	if branchName := tq.GetBranchName(); branchName != "" {
		q.AddWhereClause("branch_name = ?", branchName)
	}

	if command := tq.GetCommand(); command != "" {
		q.AddWhereClause("command = ?", command)
	}

	if pattern := tq.GetPattern(); pattern != "" {
		q.AddWhereClause("pattern = ?", pattern)
	}

	if tags := tq.GetTags(); len(tags) > 0 {
		if i.isOLAPDBEnabled() {
			clause, args := invocation_format.GetTagsAsClickhouseWhereClause("tags", tags)
			q.AddWhereClause(clause, args...)
		} else {
			return status.InvalidArgumentError("Tag filtering isn't supported without an OLAP DB.")
		}
	}

	if commitSHA := tq.GetCommitSha(); commitSHA != "" {
		q.AddWhereClause("commit_sha = ?", commitSHA)
	}

	roleClauses := query_builder.OrClauses{}
	for _, role := range tq.GetRole() {
		roleClauses.AddOr("role = ?", role)
	}
	if roleQuery, roleArgs := roleClauses.Build(); roleQuery != "" {
		q.AddWhereClause("("+roleQuery+")", roleArgs...)
	}

	if start := tq.GetUpdatedAfter(); start.IsValid() {
		q.AddWhereClause("updated_at_usec >= ?", start.AsTime().UnixMicro())
	} else {
		// If no start time specified, default to 7 days.
		lookbackWindowHours := 7 * 24 * time.Hour
		q.AddWhereClause("updated_at_usec >= ?", time.Now().Add(-lookbackWindowHours).UnixMicro())
	}

	if end := tq.GetUpdatedBefore(); end.IsValid() {
		q.AddWhereClause("updated_at_usec < ?", end.AsTime().UnixMicro())
	}

	statusClauses := toStatusClauses(tq.GetStatus())
	statusQuery, statusArgs := statusClauses.Build()
	if statusQuery != "" {
		q.AddWhereClause(fmt.Sprintf("(%s)", statusQuery), statusArgs...)
	}

	if tq.GetMinimumDuration().GetSeconds() != 0 {
		q.AddWhereClause(`duration_usec >= ?`, tq.GetMinimumDuration().AsDuration().Microseconds())
	}
	if tq.GetMaximumDuration().GetSeconds() != 0 {
		q.AddWhereClause(`duration_usec <= ?`, tq.GetMaximumDuration().AsDuration().Microseconds())
	}

	for _, f := range tq.GetFilter() {
		str, args, err := filter.GenerateFilterStringAndArgs(f, "")
		if err != nil {
			return err
		}
		q.AddWhereClause(str, args...)
	}

	q.AddWhereClause(`group_id = ?`, reqCtx.GetGroupId())
	return nil
}

func (i *InvocationStatService) getInvocationSummary(ctx context.Context, req *stpb.GetTrendRequest) (*stpb.Summary, error) {
	if !i.isOLAPDBEnabled() {
		// Invocation Summary is only available with OLAP DB enabled.
		return nil, nil
	}

	startTime := req.GetQuery().GetUpdatedAfter().AsTime()

	dataAvailableTime := time.UnixMicro(*invocationSummaryAvailableUsec)
	if dataAvailableTime.After(startTime) {
		return nil, nil
	}

	q := query_builder.NewQuery(`
    SELECT
	    count(1) AS num_builds,
		countIf(download_outputs_option IN (2, 3, 4)) as num_builds_with_remote_cache,
		sum(total_cached_action_exec_usec) as cpu_micros_saved,
		sum(action_cache_hits) as ac_cache_hits,
		sum(action_cache_misses) as ac_cache_misses
	FROM Invocations
    `)

	reqCtx := req.GetRequestContext()
	if err := i.addWhereClauses(q, req.GetQuery(), reqCtx); err != nil {
		return nil, err
	}
	qStr, qArgs := q.Build()
	row := &stpb.Summary{}
	err := i.olapdbh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("query_invocation_summary"), qStr, qArgs...).Take(row).Error
	if err != nil {
		return nil, err
	}
	return row, nil
}

func (i *InvocationStatService) getInvocationTrend(ctx context.Context, req *stpb.GetTrendRequest, timeSettings *trendTimeSettings) ([]*stpb.TrendStat, error) {
	reqCtx := req.GetRequestContext()

	q := query_builder.NewQueryWithArgs(i.getTrendBasicQuery(req.GetQuery(), timeSettings, reqCtx.GetTimezoneOffsetMinutes()))
	if err := i.addWhereClauses(q, req.GetQuery(), reqCtx); err != nil {
		return nil, err
	}
	if i.finerTimeBucketsEnabled() {
		q.SetGroupBy("bucket_start_time_micros")
	} else {
		q.SetGroupBy("name")
	}

	qStr, qArgs := q.Build()
	if i.isInvocationPercentilesEnabled() {
		qStr = i.flattenTrendsQuery(qStr)
	}

	var rows *sql.Rows
	var err error
	if i.isOLAPDBEnabled() {
		rows, err = i.olapdbh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("query_invocation_trends"), qStr, qArgs...).Rows()
	} else {
		rows, err = i.dbh.RawWithOptions(ctx, db.Opts().WithQueryName("query_invocation_trends"), qStr, qArgs...).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]*stpb.TrendStat, 0)

	for rows.Next() {
		stat := &stpb.TrendStat{}
		if i.isOLAPDBEnabled() {
			if err := i.olapdbh.DB(ctx).ScanRows(rows, &stat); err != nil {
				return nil, err
			}
		} else {
			if err := i.dbh.DB(ctx).ScanRows(rows, &stat); err != nil {
				return nil, err
			}
		}
		res = append(res, stat)
	}
	if err := rows.Err(); err != nil {
		log.Errorf("Encountered error when scan rows: %s", err)
	}
	sort.Slice(res, func(i, j int) bool {
		// Name is a date of the form "YYYY-MM-DD" so lexicographic
		// sorting is correct.
		return res[i].Name < res[j].Name
	})
	return res, nil
}

func (i *InvocationStatService) getExecutionTrendQuery(timeSettings *trendTimeSettings, timezoneOffsetMinutes int32) (string, []interface{}) {
	if !i.finerTimeBucketsEnabled() {
		return fmt.Sprintf("SELECT %s as name,", i.olapdbh.DateFromUsecTimestamp("updated_at_usec", timezoneOffsetMinutes)) + `
		quantilesExactExclusive(0.5, 0.75, 0.9, 0.95, 0.99)(IF(worker_start_timestamp_usec > queued_timestamp_usec, worker_start_timestamp_usec - queued_timestamp_usec, 0)) AS queue_duration_usec_quantiles
		FROM "Executions"`, make([]interface{}, 0)
	}

	bucketStr, bucketArgs := i.olapdbh.BucketFromUsecTimestamp("updated_at_usec", timeSettings.location, timeSettings.interval.ClickhouseInterval())

	return fmt.Sprintf("SELECT %s as bucket_start_time_micros,", bucketStr) + `
	quantilesExactExclusive(0.5, 0.75, 0.9, 0.95, 0.99)(IF(worker_start_timestamp_usec > queued_timestamp_usec, worker_start_timestamp_usec - queued_timestamp_usec, 0)) AS queue_duration_usec_quantiles
	FROM "Executions"
	`, bucketArgs
}

// The innerQuery is expected to return rows with the following columns:
//
//	(1) name; and
//	(2) queue_duration_usec_quantiles, an array of p50, p75, p90, p95, p99
//	queue duration.
//
// The returned "flattened" query will return row with the following column
//
//	name | p50 | ... | p99
func getQueryWithFlattenedArray(innerQuery string) string {
	var q string
	if *finerTimeBuckets {
		q = "SELECT bucket_start_time_micros,"
	} else {
		q = "SELECT name,"
	}
	return q + `
	arrayElement(queue_duration_usec_quantiles, 1) as queue_duration_usec_p50,
	arrayElement(queue_duration_usec_quantiles, 2) as queue_duration_usec_p75,
	arrayElement(queue_duration_usec_quantiles, 3) as queue_duration_usec_p90,
	arrayElement(queue_duration_usec_quantiles, 4) as queue_duration_usec_p95,
	arrayElement(queue_duration_usec_quantiles, 5) as queue_duration_usec_p99
	FROM (` + innerQuery + ")"
}

func (i *InvocationStatService) getExecutionTrend(ctx context.Context, req *stpb.GetTrendRequest, timeSettings *trendTimeSettings) ([]*stpb.ExecutionStat, error) {
	if !i.isOLAPDBEnabled() || !*executionTrendsEnabled {
		return nil, nil
	}
	reqCtx := req.GetRequestContext()

	q := query_builder.NewQueryWithArgs(i.getExecutionTrendQuery(timeSettings, reqCtx.GetTimezoneOffsetMinutes()))
	if err := i.addWhereClauses(q, req.GetQuery(), req.GetRequestContext()); err != nil {
		return nil, err
	}
	if *finerTimeBuckets {
		q.SetGroupBy("bucket_start_time_micros")
	} else {
		q.SetGroupBy("name")
	}

	qStr, qArgs := q.Build()
	qStr = getQueryWithFlattenedArray(qStr)
	rows, err := i.olapdbh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("query_execution_trends"), qStr, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	res := make([]*stpb.ExecutionStat, 0)

	for rows.Next() {
		stat := &stpb.ExecutionStat{}
		if err := i.olapdbh.DB(ctx).ScanRows(rows, &stat); err != nil {
			return nil, err
		}
		res = append(res, stat)
	}
	if err := rows.Err(); err != nil {
		log.Errorf("Encountered error when scan rows: %s", err)
	}
	sort.Slice(res, func(i, j int) bool {
		// Name is a date of the form "YYYY-MM-DD" so lexicographic
		// sorting is correct.
		return res[i].Name < res[j].Name
	})
	return res, nil
}

func (i *InvocationStatService) GetTrend(ctx context.Context, req *stpb.GetTrendRequest) (*stpb.GetTrendResponse, error) {
	if err := perms.AuthorizeGroupAccessForStats(ctx, i.env, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	// Normalize repo URL before we use it in any queries.
	if repoURL := req.GetQuery().GetRepoUrl(); repoURL != "" {
		repoURL, err := git.NormalizeRepoURL(repoURL)
		if err == nil {
			req = proto.Clone(req).(*stpb.GetTrendRequest)
			req.Query.RepoUrl = repoURL.String()
		}
	}

	rsp := &stpb.GetTrendResponse{}

	timeSettings := i.getTrendTimeSettings(req.GetQuery(), req.GetRequestContext().GetTimezone())
	rsp.Interval = timeSettings.interval.IntervalProto()

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		if rsp.CurrentSummary, err = i.getInvocationSummary(ctx, req); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		var err error
		// TODO(jdhollen): This is a little funky: updated_after is set to midnight
		// local time, so the subtraction we do below ends up being "last 29 days
		// plus a few extra hours depending on what time of day it is".  There isn't
		// really a "correct" solution, because we don't have a full day of data,
		// but maybe we will decide we like showing the full previous 30 days.
		endTime := time.Now()
		if end := req.GetQuery().GetUpdatedBefore(); end.IsValid() {
			endTime = end.AsTime()
		}
		startTime := req.GetQuery().GetUpdatedAfter().AsTime()
		duration := endTime.Sub(startTime)
		endTime = startTime
		startTime = endTime.Add(-duration)
		newReq := proto.Clone(req).(*stpb.GetTrendRequest)
		if newReq.Query == nil {
			newReq.Query = &stpb.TrendQuery{}
		}
		newReq.GetQuery().UpdatedBefore = timestamppb.New(endTime)
		newReq.GetQuery().UpdatedAfter = timestamppb.New(startTime)
		if rsp.PreviousSummary, err = i.getInvocationSummary(ctx, newReq); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		var err error
		if rsp.TrendStat, err = i.getInvocationTrend(ctx, req, timeSettings); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		var err error
		if rsp.ExecutionStat, err = i.getExecutionTrend(ctx, req, timeSettings); err != nil {
			return err
		}
		return nil
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}
	if i.isInvocationPercentilesEnabled() {
		rsp.HasInvocationStatPercentiles = true
	}
	return rsp, nil
}

func (i *InvocationStatService) GetInvocationStatBaseQuery(aggColumn string) string {
	q := fmt.Sprintf("SELECT %s as name,", aggColumn)
	if i.isOLAPDBEnabled() {
		q = q + `
	    SUM(duration_usec) as total_build_time_usec,`
	} else {
		q = q + `
	    SUM(CASE WHEN duration_usec > 0 THEN duration_usec END) as total_build_time_usec,`
	}
	q = q + `
	    MAX(updated_at_usec) as latest_build_time_usec,
	    MAX(CASE WHEN (success AND invocation_status = 1) THEN updated_at_usec END) as last_green_build_usec,
	    MAX(CASE WHEN (success != true AND invocation_status = 1) THEN updated_at_usec END) as last_red_build_usec,
	    COUNT(1) as total_num_builds,
	    COUNT(CASE WHEN (success AND invocation_status = 1) THEN 1 END) as total_num_sucessful_builds,
	    COUNT(CASE WHEN (success != true AND invocation_status = 1) THEN 1 END) as total_num_failing_builds,
	    SUM(action_count) as total_actions
            FROM "Invocations"`
	return q
}

func getTableForMetric(metric *sfpb.Metric) string {
	if metric.Execution != nil {
		return "Executions"
	}
	return "Invocations"
}

type MetricRange = struct {
	Start      int64
	BucketSize int64
	NumBuckets int64
}

func (i *InvocationStatService) getMetricRange(ctx context.Context, table string, metric string, whereClauseStr string, whereClauseArgs []interface{}) (*MetricRange, error) {
	rangeQuery := fmt.Sprintf(`SELECT min(%s) as low, max(%s) as high FROM "%s" %s`, metric, metric, table, whereClauseStr)
	var rows *sql.Rows
	rows, err := i.olapdbh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("query_metric_range"), rangeQuery, whereClauseArgs...).Rows()
	if err != nil {
		return nil, err
	}
	if !rows.Next() {
		return nil, nil
	}
	valueRange := struct {
		Low  int64
		High int64
	}{}
	if err := i.olapdbh.DB(ctx).ScanRows(rows, &valueRange); err != nil {
		return nil, err
	}

	// A fun hack: make "High" value 1 higher so that we can put an exclusive
	// limit on the upper bound of requested ranges. Each bucket is [min, max).
	width := valueRange.High + 1 - valueRange.Low
	var step int64

	bucketCount := int64(20)
	if width <= bucketCount {
		step = 1
	} else {
		step = width / bucketCount
		if (width % bucketCount) != 0 {
			step = step + 1
		}
	}

	return &MetricRange{
			Start:      valueRange.Low,
			BucketSize: step,
			NumBuckets: bucketCount,
		},
		nil
}

func (i *InvocationStatService) getMetricBuckets(ctx context.Context, table string, metric string, whereClauseStr string, whereClauseArgs []interface{}) ([]int64, string, error) {
	metricRange, err := i.getMetricRange(ctx, table, metric, whereClauseStr, whereClauseArgs)
	if err != nil {
		return nil, "", err
	}
	if metricRange == nil {
		return nil, "", nil
	}

	metricBuckets := make([]int64, metricRange.NumBuckets+1)
	for i := range metricBuckets[:] {
		metricBuckets[i] = metricRange.Start + int64(i)*metricRange.BucketSize
	}
	mStr := make([]string, metricRange.NumBuckets)
	for i := range mStr {
		mStr[i] = fmt.Sprint(metricBuckets[i])
	}
	metricArrayStr := "array(" + strings.Join(mStr, ",") + ")"
	return metricBuckets, metricArrayStr, nil
}

// Given a duration for which we are computing a heatmap, return an
// appropriate fixed bucket size for a heatmap.
func computeTimeBucketSize(duration time.Duration) time.Duration {
	if duration <= 16*time.Hour {
		return 5 * time.Minute
	}
	if duration <= 48*time.Hour {
		return 15 * time.Minute
	}
	if duration <= 96*time.Hour {
		return 30 * time.Minute
	}
	return 1 * time.Hour
}

const ONE_WEEK = 7 * 24 * time.Hour

func (i *InvocationStatService) getTimestampBuckets(q *stpb.TrendQuery, requestContext *ctxpb.RequestContext) ([]int64, string, error) {
	lowTime := q.GetUpdatedAfter()
	highTime := q.GetUpdatedBefore()
	if lowTime != nil && !lowTime.IsValid() {
		return nil, "", status.InvalidArgumentError(fmt.Sprintf("Invalid low timestamp: %v", lowTime))
	}
	if highTime != nil && (!highTime.IsValid() || highTime.AsTime().Unix() < lowTime.AsTime().Unix()) {
		return nil, "", status.InvalidArgumentError(fmt.Sprintf("Invalid high timestamp: %v", highTime))
	}

	startSec := time.Now().Unix() - int64(ONE_WEEK.Seconds())
	if lowTime != nil {
		startSec = lowTime.GetSeconds()
	}

	endSec := time.Now().Unix()
	if highTime != nil {
		endSec = highTime.GetSeconds()
	}

	timezoneOffset := time.Duration(requestContext.GetTimezoneOffsetMinutes()) * time.Minute
	timezone := requestContext.GetTimezone()

	loc := time.FixedZone("Fixed Offset", -int(timezoneOffset.Seconds()))
	// Find the user's timezone. time.LoadLocation defaults the empty string to
	// UTC, so we need a special case to ignore it.
	if *useTimezoneInHeatmapQueries && timezone != "" {
		// If you don't like this variable name, message tylerwilliams
		if locedAndLoaded, err := time.LoadLocation(timezone); err == nil {
			loc = locedAndLoaded
		}
	}

	start := time.Unix(startSec, 0).In(loc)
	end := time.Unix(endSec, 0).In(loc)

	var timestampBuckets []int64
	timestampBuckets = append(timestampBuckets, start.UnixMicro())
	// When the queried time range is less than eight days, we show smaller buckets.
	const eightDays = 8 * 24 * time.Hour
	queriedDuration := end.Sub(start)
	if i.finerTimeBucketsEnabled() && queriedDuration <= eightDays {
		increment := computeTimeBucketSize(time.Duration(endSec-startSec) * time.Second)
		current := start.Round(increment)
		for current.Before(start) || current.Equal(start) {
			current = current.Add(increment)
		}
		for current.Before(end) {
			timestampBuckets = append(timestampBuckets, current.UnixMicro())
			current = current.Add(increment)
		}
	} else {
		// Each subsequent bucket will start at midnight on the following day.
		midnightOnStartDate := time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, start.Location())
		current := midnightOnStartDate.AddDate(0, 0, 1)
		for current.Before(end) {
			timestampBuckets = append(timestampBuckets, current.UnixMicro())
			current = current.AddDate(0, 0, 1)
		}
	}
	timestampBuckets = append(timestampBuckets, end.UnixMicro())

	numDateBuckets := len(timestampBuckets) - 1

	dStr := make([]string, numDateBuckets)
	for i, d := range timestampBuckets[:numDateBuckets] {
		dStr[i] = fmt.Sprintf("%d", d)
	}
	timestampArrayStr := fmt.Sprintf("array(%s)", strings.Join(dStr, ","))

	return timestampBuckets, timestampArrayStr, nil
}

type HeatmapQueryInputs = struct {
	PlaceholderBucketQuery string
	TimestampBuckets       []int64
	TimestampArrayStr      string
	MetricBuckets          []int64
	MetricArrayStr         string
}

func (i *InvocationStatService) generateQueryInputs(ctx context.Context, table string, metric string, q *stpb.TrendQuery, whereClauseStr string, whereClauseArgs []interface{}, requestContext *ctxpb.RequestContext) (*HeatmapQueryInputs, error) {
	timestampBuckets, timestampArrayStr, err := i.getTimestampBuckets(q, requestContext)
	if err != nil {
		return nil, err
	}
	metricBuckets, metricArrayStr, err := i.getMetricBuckets(ctx, table, metric, whereClauseStr, whereClauseArgs)
	if err != nil {
		return nil, err
	}
	if len(metricBuckets) == 0 {
		return nil, nil
	}

	pbq := fmt.Sprintf(`
	  SELECT toInt64(timestamp) as timestamp, toInt64(bucket) as bucket, 0 as v FROM (
			SELECT arrayElement(%s, number + 1) AS timestamp FROM numbers(%d)) AS a
		CROSS JOIN (
			SELECT arrayElement(%s, number + 1) AS bucket FROM numbers(%d)) AS b
		ORDER BY timestamp, bucket`,
		timestampArrayStr, len(timestampBuckets)-1, metricArrayStr, len(metricBuckets)-1)

	return &HeatmapQueryInputs{
		PlaceholderBucketQuery: pbq,
		TimestampBuckets:       timestampBuckets,
		TimestampArrayStr:      timestampArrayStr,
		MetricBuckets:          metricBuckets,
		MetricArrayStr:         metricArrayStr}, nil
}

func (i *InvocationStatService) getWhereClauseForHeatmapQuery(m *sfpb.Metric, q *stpb.TrendQuery, reqCtx *ctxpb.RequestContext) (string, []interface{}, error) {
	placeholderQuery := query_builder.NewQuery("")
	if err := i.addWhereClauses(placeholderQuery, q, reqCtx); err != nil {
		return "", nil, err
	}
	if m.GetInvocation() == sfpb.InvocationMetricType_DURATION_USEC_INVOCATION_METRIC {
		placeholderQuery.AddWhereClause("duration_usec > 0")
	}
	whereString, whereArgs := placeholderQuery.Build()
	return whereString, whereArgs, nil
}

type QueryAndBuckets = struct {
	Query            string
	QueryArgs        []interface{}
	TimestampBuckets []int64
	MetricBuckets    []int64
}

// getHeatmapQueryAndBuckets usually returns the query that should be run to
// fetch the heatmap and the buckets that define the heatmap range, but in the
// event that no events are found at all, it will instead return (nil, nil) to
// indicate a no-error state with no results--in this case we should return an
// empty response.
func (i *InvocationStatService) getHeatmapQueryAndBuckets(ctx context.Context, req *stpb.GetStatHeatmapRequest) (*QueryAndBuckets, error) {
	table := getTableForMetric(req.GetMetric())
	metric, err := filter.MetricToDbField(req.GetMetric(), "")
	if err != nil {
		return nil, err
	}
	whereClauseStr, whereClauseArgs, err := i.getWhereClauseForHeatmapQuery(req.GetMetric(), req.GetQuery(), req.GetRequestContext())
	if err != nil {
		return nil, err
	}

	qi, err := i.generateQueryInputs(ctx, table, metric, req.GetQuery(), whereClauseStr, whereClauseArgs, req.GetRequestContext())
	if err != nil {
		return nil, err
	}
	if qi == nil {
		return nil, nil
	}
	qStr := fmt.Sprintf(`
		SELECT timestamp, groupArray(v) AS value FROM (
			SELECT timestamp, bucket, CAST(SUM(v) AS Int64) AS v FROM (
				%s UNION ALL
				SELECT
					roundDown(updated_at_usec, CAST(%s AS Array(Int64))) AS timestamp,
					roundDown(%s, CAST(%s AS Array(Int64))) AS bucket,
					count(*) AS v
					FROM "%s" %s
					GROUP BY timestamp, bucket)
			GROUP BY timestamp, bucket ORDER BY timestamp, bucket)
		GROUP BY timestamp ORDER BY timestamp`,
		qi.PlaceholderBucketQuery, qi.TimestampArrayStr, metric, qi.MetricArrayStr, table, whereClauseStr)

	return &QueryAndBuckets{
			TimestampBuckets: qi.TimestampBuckets,
			MetricBuckets:    qi.MetricBuckets,
			Query:            qStr,
			QueryArgs:        whereClauseArgs,
		},
		nil
}

func (i *InvocationStatService) GetStatHeatmap(ctx context.Context, req *stpb.GetStatHeatmapRequest) (*stpb.GetStatHeatmapResponse, error) {
	if !config.TrendsHeatmapEnabled() {
		return nil, status.UnimplementedError("Stat heatmaps are not enabled.")
	}
	if !i.isOLAPDBEnabled() {
		return nil, status.UnimplementedError("Time series charts require using an OLAP DB, but none is configured.")
	}
	if err := perms.AuthorizeGroupAccessForStats(ctx, i.env, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	qAndBuckets, err := i.getHeatmapQueryAndBuckets(ctx, req)
	if err != nil {
		return nil, err
	}
	if qAndBuckets == nil {
		// There are no stats in the requested window; send an empty response.
		return &stpb.GetStatHeatmapResponse{}, nil
	}

	var rows *sql.Rows
	rows, err = i.olapdbh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("query_stat_heatmap"), qAndBuckets.Query, qAndBuckets.QueryArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rsp := &stpb.GetStatHeatmapResponse{
		TimestampBracket: qAndBuckets.TimestampBuckets,
		BucketBracket:    qAndBuckets.MetricBuckets,
		Column:           make([]*stpb.HeatmapColumn, 0),
	}

	for rows.Next() {
		stat := struct {
			Timestamp int64
			Value     []int64 `gorm:"type:int64[]"`
		}{}
		if err := i.olapdbh.DB(ctx).ScanRows(rows, &stat); err != nil {
			return nil, err
		}
		column := &stpb.HeatmapColumn{
			TimestampUsec: stat.Timestamp,
			Value:         stat.Value,
		}
		rsp.Column = append(rsp.Column, column)
	}
	return rsp, nil
}

func (i *InvocationStatService) GetInvocationStat(ctx context.Context, req *inpb.GetInvocationStatRequest) (*inpb.GetInvocationStatResponse, error) {
	if req.GetAggregationType() == inpb.AggType_UNKNOWN_AGGREGATION_TYPE {
		return nil, status.InvalidArgumentError("A valid aggregation type must be provided")
	}

	groupID := req.GetRequestContext().GetGroupId()
	if err := perms.AuthorizeGroupAccessForStats(ctx, i.env, groupID); err != nil {
		return nil, err
	}

	limit := int32(100)
	if l := req.GetLimit(); l != 0 {
		if l < 1 || l > 1000 {
			return nil, status.InvalidArgumentErrorf("limit must be between 0 and 1000")
		}
		limit = l
	}

	aggColumn, err := i.getAggColumn(req.GetRequestContext(), req.AggregationType)
	if err != nil {
		return nil, err
	}
	q := query_builder.NewQuery(i.GetInvocationStatBaseQuery(aggColumn))

	if req.AggregationType != inpb.AggType_DATE_AGGREGATION_TYPE {
		q.AddWhereClause(`? != ''`, aggColumn)
	}

	if user := req.GetQuery().GetUser(); user != "" {
		q.AddWhereClause("user = ?", user)
	}

	if host := req.GetQuery().GetHost(); host != "" {
		q.AddWhereClause("host = ?", host)
	}

	if repoURL := req.GetQuery().GetRepoUrl(); repoURL != "" {
		q.AddWhereClause("repo_url = ?", repoURL)
	}

	if branchName := req.GetQuery().GetBranchName(); branchName != "" {
		q.AddWhereClause("branch_name = ?", branchName)
	}

	if command := req.GetQuery().GetCommand(); command != "" {
		q.AddWhereClause("command = ?", command)
	}

	if pattern := req.GetQuery().GetPattern(); pattern != "" {
		q.AddWhereClause("pattern = ?", pattern)
	}

	if tags := req.GetQuery().GetTags(); len(tags) > 0 {
		if i.isOLAPDBEnabled() {
			clause, args := invocation_format.GetTagsAsClickhouseWhereClause("tags", tags)
			q.AddWhereClause(clause, args...)
		} else {
			return nil, status.InvalidArgumentError("Tag filtering isn't supported without an OLAP DB.")
		}
	}

	if commitSHA := req.GetQuery().GetCommitSha(); commitSHA != "" {
		q.AddWhereClause("commit_sha = ?", commitSHA)
	}

	roleClauses := query_builder.OrClauses{}
	for _, role := range req.GetQuery().GetRole() {
		roleClauses.AddOr("role = ?", role)
	}
	if roleQuery, roleArgs := roleClauses.Build(); roleQuery != "" {
		q.AddWhereClause("("+roleQuery+")", roleArgs...)
	}

	if start := req.GetQuery().GetUpdatedAfter(); start.IsValid() {
		q.AddWhereClause("updated_at_usec >= ?", start.AsTime().UnixMicro())
	}

	if end := req.GetQuery().GetUpdatedBefore(); end.IsValid() {
		q.AddWhereClause("updated_at_usec < ?", end.AsTime().UnixMicro())
	}

	for _, f := range req.GetQuery().GetFilter() {
		str, args, err := filter.GenerateFilterStringAndArgs(f, "")
		if err != nil {
			return nil, err
		}
		q.AddWhereClause(str, args...)
	}

	statusClauses := toStatusClauses(req.GetQuery().GetStatus())
	statusQuery, statusArgs := statusClauses.Build()
	if statusQuery != "" {
		q.AddWhereClause(fmt.Sprintf("(%s)", statusQuery), statusArgs...)
	}

	q.AddWhereClause(`group_id = ?`, groupID)
	q.SetGroupBy("name")
	q.SetOrderBy("latest_build_time_usec" /*ascending=*/, false)
	q.SetLimit(int64(limit))

	qStr, qArgs := q.Build()
	var rows *sql.Rows
	if i.isOLAPDBEnabled() {
		rows, err = i.olapdbh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("query_invocation_stats"), qStr, qArgs...).Rows()
	} else {
		rows, err = i.dbh.RawWithOptions(ctx, db.Opts().WithQueryName("query_invocation_stats"), qStr, qArgs...).Rows()
	}
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rsp := &inpb.GetInvocationStatResponse{}
	rsp.InvocationStat = make([]*inpb.InvocationStat, 0)

	for rows.Next() {
		stat := &inpb.InvocationStat{}
		if i.isOLAPDBEnabled() {
			if err := i.olapdbh.DB(ctx).ScanRows(rows, &stat); err != nil {
				return nil, err
			}
		} else {
			if err := i.dbh.DB(ctx).ScanRows(rows, &stat); err != nil {
				return nil, err
			}
		}
		rsp.InvocationStat = append(rsp.InvocationStat, stat)
	}
	return rsp, nil
}

func (i *InvocationStatService) getDrilldownSubquery(ctx context.Context, drilldownFields []string, req *stpb.GetStatDrilldownRequest, where string, whereArgs []interface{}, drilldown string, drilldownArgs []interface{}, col string) (string, []interface{}) {
	// This is really ugly--the clickhouse SQL engine doesn't play nice with GORM
	// when you set a field name to NULL that matches a field in the table you are
	// selecting--it blows away the type info and turns it into Sql.RawBytes.  To
	// work around this, we prefix the  fields in our final query with 'gorm_'.
	queryFields := make([]string, len(drilldownFields))
	for i, f := range drilldownFields {
		if f != col {
			queryFields[i] = "NULL as gorm_" + f
		} else if col == "tag" {
			queryFields[i] = "arrayJoin(tags) as gorm_tag"
		} else {
			queryFields[i] = f + " as gorm_" + f
		}
	}
	nulledOutFieldList := strings.Join(queryFields, ", ")

	table := getTableForMetric(req.GetDrilldownMetric())

	args := append(drilldownArgs, append(drilldownArgs, whereArgs...)...)

	// More filth: we want the total row that counts up the total number of
	// invocations / executions in each group to come first in the results so that
	// we can use it as an input into how we rank the charts.  To do this, we
	// toss in a bogus column and sort by it to ensure that totals come first.
	if col == "" {
		return fmt.Sprintf(
			`(SELECT %s, 0 AS totals_first, count(*) AS total,
					countIf(%s) AS selection, countIf(not(%s)) AS inverse
				FROM "%s" %s)`,
			nulledOutFieldList, drilldown, drilldown, table, where), args
	}
	col = "gorm_" + col

	return fmt.Sprintf(`
		(SELECT %s, 1 AS totals_first, count(*) AS total, countIf(%s) AS selection,
			countIf(not(%s)) AS inverse
		FROM "%s" %s
		GROUP BY %s ORDER BY selection DESCENDING, total DESCENDING LIMIT 25)`,
		nulledOutFieldList, drilldown, drilldown, table, where, col), args
}

func getDrilldownQueryFilter(filters []*sfpb.StatFilter) (string, []interface{}, error) {
	if len(filters) == 0 {
		return "", nil, status.InvalidArgumentError("Empty filter for drilldown.")
	}
	var result []string
	var resultArgs []interface{}
	for _, f := range filters[:] {
		str, args, err := filter.GenerateFilterStringAndArgs(f, "")
		if err != nil {
			return "", nil, err
		}
		result = append(result, str)
		resultArgs = append(resultArgs, args...)
	}
	return strings.Join(result, " AND "), resultArgs, nil
}

// TODO(jdhollen): This can be made much efficient using GROUPING SETS when we
// are able to upgrade to clickhouse 22.6 or later.  The release date for 22.8
// from Altinity is supposed to be 2023-02-15.
func (i *InvocationStatService) getDrilldownQuery(ctx context.Context, req *stpb.GetStatDrilldownRequest) (string, []interface{}, error) {
	drilldownFields := []string{"user", "host", "pattern", "repo_url", "branch_name", "commit_sha"}
	if *tagsInDrilldowns {
		drilldownFields = append(drilldownFields, "tag")
	}
	if req.GetDrilldownMetric().Execution != nil {
		drilldownFields = append(drilldownFields, "worker")
	}
	placeholderQuery := query_builder.NewQuery("")

	if err := i.addWhereClauses(placeholderQuery, req.GetQuery(), req.GetRequestContext()); err != nil {
		return "", nil, err
	}

	drilldownStr, drilldownArgs, err := getDrilldownQueryFilter(req.GetFilter())
	if err != nil {
		return "", nil, err
	}

	whereString, whereArgs := placeholderQuery.Build()

	args := make([]interface{}, 0)
	queries := make([]string, 0)
	subQStr, subQArgs := i.getDrilldownSubquery(ctx, drilldownFields, req, whereString, whereArgs, drilldownStr, drilldownArgs, "")
	queries = append(queries, subQStr)
	args = append(args, subQArgs...)
	for _, s := range drilldownFields {
		subQStr, subQArgs := i.getDrilldownSubquery(ctx, drilldownFields, req, whereString, whereArgs, drilldownStr, drilldownArgs, s)
		queries = append(queries, subQStr)
		args = append(args, subQArgs...)
	}

	return fmt.Sprintf("SELECT * FROM (%s) ORDER BY totals_first", strings.Join(queries, " UNION ALL ")), args, nil
}

func addOutputChartEntry(m map[stpb.DrilldownType]*stpb.DrilldownChart, dm map[stpb.DrilldownType]float64, ddType stpb.DrilldownType, label *string, inverse int64, selection int64, totalInBase int64, totalInSelection int64) {
	chart, exists := m[ddType]
	if !exists {
		chart = &stpb.DrilldownChart{}
		chart.DrilldownType = ddType
		m[ddType] = chart
		dm[ddType] = -math.MaxFloat64
	}
	dm[ddType] = math.Max(dm[ddType], math.Abs(float64(selection)/float64(totalInSelection)-float64(inverse)/float64(totalInBase)))
	chart.Entry = append(chart.Entry, &stpb.DrilldownEntry{Label: *label, BaseValue: inverse, SelectionValue: selection})
}

func sortDrilldownChartKeys(dm map[stpb.DrilldownType]float64) *[]stpb.DrilldownType {
	type pair struct {
		a stpb.DrilldownType
		v float64
	}
	slice := make([]pair, 0)
	for k, v := range dm {
		slice = append(slice, pair{k, v})
	}
	sort.SliceStable(slice, func(a, b int) bool {
		return slice[a].v >= slice[b].v
	})

	result := make([]stpb.DrilldownType, len(slice))
	for v, i := range slice {
		result[v] = i.a
	}
	return &result
}

func (i *InvocationStatService) GetStatDrilldown(ctx context.Context, req *stpb.GetStatDrilldownRequest) (*stpb.GetStatDrilldownResponse, error) {
	if !config.TrendsHeatmapEnabled() {
		return nil, status.UnimplementedError("Stat heatmaps are not enabled.")
	}
	if !i.isOLAPDBEnabled() {
		return nil, status.UnimplementedError("Time series charts require using an OLAP DB, but none is configured.")
	}
	if err := perms.AuthorizeGroupAccessForStats(ctx, i.env, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	qStr, qArgs, err := i.getDrilldownQuery(ctx, req)
	if err != nil {
		return nil, err
	}

	var rows *sql.Rows
	rows, err = i.olapdbh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("query_stat_drilldown"), qStr, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rsp := &stpb.GetStatDrilldownResponse{}

	rsp.Chart = make([]*stpb.DrilldownChart, 0)
	m := make(map[stpb.DrilldownType]*stpb.DrilldownChart)
	dm := make(map[stpb.DrilldownType]float64)
	type queryOut struct {
		GormUser       *string
		GormHost       *string
		GormRepoURL    *string
		GormBranchName *string
		GormCommitSHA  *string
		GormPattern    *string
		GormWorker     *string
		GormTag        *string
		Selection      int64
		Inverse        int64
	}
	if !rows.Next() {
		return rsp, nil
	}
	totals := queryOut{}
	if err := i.olapdbh.DB(ctx).ScanRows(rows, &totals); err != nil {
		return nil, err
	}
	if totals.GormUser != nil || totals.GormHost != nil || totals.GormRepoURL != nil || totals.GormBranchName != nil || totals.GormCommitSHA != nil {
		return nil, status.InternalError("Failed to fetch drilldown data")
	}
	rsp.TotalInBase = totals.Inverse
	rsp.TotalInSelection = totals.Selection

	for rows.Next() {
		stat := queryOut{}
		if err := i.olapdbh.DB(ctx).ScanRows(rows, &stat); err != nil {
			return nil, err
		}

		if stat.GormBranchName != nil {
			addOutputChartEntry(m, dm, stpb.DrilldownType_BRANCH_DRILLDOWN_TYPE, stat.GormBranchName, stat.Inverse, stat.Selection, rsp.TotalInBase, rsp.TotalInSelection)
		} else if stat.GormHost != nil {
			addOutputChartEntry(m, dm, stpb.DrilldownType_HOSTNAME_DRILLDOWN_TYPE, stat.GormHost, stat.Inverse, stat.Selection, rsp.TotalInBase, rsp.TotalInSelection)
		} else if stat.GormRepoURL != nil {
			addOutputChartEntry(m, dm, stpb.DrilldownType_REPO_URL_DRILLDOWN_TYPE, stat.GormRepoURL, stat.Inverse, stat.Selection, rsp.TotalInBase, rsp.TotalInSelection)
		} else if stat.GormUser != nil {
			addOutputChartEntry(m, dm, stpb.DrilldownType_USER_DRILLDOWN_TYPE, stat.GormUser, stat.Inverse, stat.Selection, rsp.TotalInBase, rsp.TotalInSelection)
		} else if stat.GormCommitSHA != nil {
			addOutputChartEntry(m, dm, stpb.DrilldownType_COMMIT_SHA_DRILLDOWN_TYPE, stat.GormCommitSHA, stat.Inverse, stat.Selection, rsp.TotalInBase, rsp.TotalInSelection)
		} else if stat.GormPattern != nil {
			addOutputChartEntry(m, dm, stpb.DrilldownType_PATTERN_DRILLDOWN_TYPE, stat.GormPattern, stat.Inverse, stat.Selection, rsp.TotalInBase, rsp.TotalInSelection)
		} else if stat.GormWorker != nil {
			addOutputChartEntry(m, dm, stpb.DrilldownType_WORKER_DRILLDOWN_TYPE, stat.GormWorker, stat.Inverse, stat.Selection, rsp.TotalInBase, rsp.TotalInSelection)
		} else if stat.GormTag != nil {
			addOutputChartEntry(m, dm, stpb.DrilldownType_TAG_DRILLDOWN_TYPE, stat.GormTag, stat.Inverse, stat.Selection, rsp.TotalInBase, rsp.TotalInSelection)
		} else {
			// The above clauses represent all of the GROUP BY options we have in our
			// query, and we deliberately constructed the query so that the total row
			// would come first--this is an unexpected state.
			return nil, status.InternalError("Failed to fetch drilldown data")
		}
	}

	order := sortDrilldownChartKeys(dm)

	for _, ddType := range *order {
		rsp.Chart = append(rsp.Chart, m[ddType])
	}
	return rsp, nil
}

func toStatusClauses(statuses []inspb.OverallStatus) *query_builder.OrClauses {
	statusClauses := &query_builder.OrClauses{}
	for _, status := range statuses {
		switch status {
		case inspb.OverallStatus_SUCCESS:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS), 1)
		case inspb.OverallStatus_FAILURE:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS), 0)
		case inspb.OverallStatus_IN_PROGRESS:
			statusClauses.AddOr(`invocation_status = ?`, int(inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS))
		case inspb.OverallStatus_DISCONNECTED:
			statusClauses.AddOr(`invocation_status = ?`, int(inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS))
		case inspb.OverallStatus_UNKNOWN_OVERALL_STATUS:
			continue
		default:
			continue
		}
	}
	return statusClauses
}

func (i *InvocationStatService) isOLAPDBEnabled() bool {
	return i.olapdbh != nil && *readFromOLAPDBEnabled
}

func (i *InvocationStatService) finerTimeBucketsEnabled() bool {
	return i.isOLAPDBEnabled() && *finerTimeBuckets
}

func (i *InvocationStatService) isInvocationPercentilesEnabled() bool {
	return i.isOLAPDBEnabled() && *invocationPercentilesEnabled
}

package execution_search_service

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"

	expb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	ispb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

const (
	defaultLimitSize     = int64(15)
	pageSizeOffsetPrefix = "offset_"

	// The maximum number of individual executions returned by
	// GetExecutionTimeline, sampled uniformly at random from all matching
	// executions.  Summary stats are computed over all matching executions in
	// the OLAP DB, so they are unaffected by this cap.
	timelineExecutionSampleSize = int64(1000)
)

var (
	runMatcher   = regexp.MustCompile(`[_/]run_\d+_of_\d+`)
	shardMatcher = regexp.MustCompile(`/shard_(\d+)_of_\d+/`)
)

type ExecutionSearchService struct {
	env environment.Env
	h   interfaces.DBHandle
	oh  interfaces.OLAPDBHandle
}

func NewExecutionSearchService(env environment.Env, h interfaces.DBHandle, oh interfaces.OLAPDBHandle) *ExecutionSearchService {
	return &ExecutionSearchService{
		env: env,
		h:   h,
		oh:  oh,
	}
}

func (s *ExecutionSearchService) rawQueryExecutions(ctx context.Context, query string, queryArgs ...interface{}) ([]*schema.Execution, error) {
	rq := s.oh.NewQuery(ctx, "execution_search_service_search").Raw(query, queryArgs...)
	return db.ScanAll(rq, &schema.Execution{})
}

func clickhouseExecutionToProto(in *schema.Execution) (*expb.ExecutionWithInvocationMetadata, error) {
	ex, err := execution.OLAPExecToClientProto(in)
	if err != nil {
		return nil, status.WrapError(err, "convert clickhouse execution to proto")
	}
	invocationID, err := uuid.Base64StringToString(in.InvocationUUID)
	if err != nil {
		return nil, status.WrapError(err, "parse invocation UUID")
	}
	return &expb.ExecutionWithInvocationMetadata{
		Execution: ex,
		InvocationMetadata: &expb.InvocationMetadata{
			Id:               invocationID,
			User:             in.User,
			Host:             in.Host,
			Pattern:          in.Pattern,
			Role:             in.Role,
			BranchName:       in.BranchName,
			CommitSha:        in.CommitSHA,
			RepoUrl:          in.RepoURL,
			Command:          in.Command,
			Success:          in.Success,
			InvocationStatus: ispb.InvocationStatus(in.InvocationStatus),
		},
	}, nil
}

func (s *ExecutionSearchService) SearchExecutions(ctx context.Context, req *expb.SearchExecutionRequest) (*expb.SearchExecutionResponse, error) {
	if s.oh == nil {
		return nil, status.UnavailableError("An OLAP DB is required to search executions.")
	}
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InvalidArgumentError("Failed to find user's group when searching executions.")
	}
	if err := authutil.AuthorizeGroupAccessForStats(ctx, s.env, u.GetGroupID()); err != nil {
		return nil, err
	}

	q := query_builder.NewQuery(`
		SELECT invocation_uuid, ` + strings.Join(execution.ExecutionListingColumns(), ", ") + `
		FROM "Executions"
	`)

	// Always filter to the currently selected (and authorized) group.
	q.AddWhereClause("group_id = ?", u.GetGroupID())
	q.AddWhereClause("invocation_uuid != ''")

	if err := s.addExecutionQueryFilters(q, req.GetQuery()); err != nil {
		return nil, err
	}

	q.SetOrderBy("created_at_usec", true)

	limitSize := defaultLimitSize
	if req.Count > 0 {
		limitSize = int64(req.Count)
	}
	q.SetLimit(limitSize)

	offset := int64(0)
	if strings.HasPrefix(req.PageToken, pageSizeOffsetPrefix) {
		parsedOffset, err := strconv.ParseInt(strings.Replace(req.PageToken, pageSizeOffsetPrefix, "", 1), 10, 64)
		if err != nil {
			return nil, status.InvalidArgumentError("Error parsing pagination token")
		}
		offset = parsedOffset
	} else if req.PageToken != "" {
		return nil, status.InvalidArgumentError("Invalid pagination token")
	}
	q.SetOffset(offset)

	qString, qArgs := q.Build()
	olapExecutions, err := s.rawQueryExecutions(ctx, qString, qArgs...)
	if err != nil {
		return nil, err
	}

	rsp := &expb.SearchExecutionResponse{
		Execution: make([]*expb.ExecutionWithInvocationMetadata, len(olapExecutions)),
	}
	for i, ex := range olapExecutions {
		converted, err := clickhouseExecutionToProto(ex)
		if err != nil {
			return nil, status.WrapError(err, "convert clickhouse execution to proto")
		}
		rsp.Execution[i] = converted
	}
	if int64(len(rsp.Execution)) == limitSize {
		rsp.NextPageToken = pageSizeOffsetPrefix + strconv.FormatInt(offset+limitSize, 10)
	}
	return rsp, nil
}

// addExecutionQueryFilters applies the WHERE clauses shared by all execution
// queries (SearchExecutions and GetExecutionTimeline) based on the fields set
// on the provided ExecutionQuery. The caller is responsible for any
// query-specific clauses (e.g. group_id, target_label) and for the SELECT,
// ORDER BY, and pagination.
func (s *ExecutionSearchService) addExecutionQueryFilters(q *query_builder.Query, query *expb.ExecutionQuery) error {
	if user := query.GetInvocationUser(); user != "" {
		q.AddWhereClause("\"user\" = ?", user)
	}
	if host := query.GetInvocationHost(); host != "" {
		q.AddWhereClause("host = ?", host)
	}
	if url := query.GetRepoUrl(); url != "" {
		q.AddWhereClause("repo_url = ?", url)
	}
	if branch := query.GetBranchName(); branch != "" {
		q.AddWhereClause("branch_name = ?", branch)
	}
	if command := query.GetCommand(); command != "" {
		q.AddWhereClause("command = ?", command)
	}
	if pattern := query.GetPattern(); pattern != "" {
		q.AddWhereClause("pattern = ?", pattern)
	}
	if sha := query.GetCommitSha(); sha != "" {
		q.AddWhereClause("commit_sha = ?", sha)
	}
	roleClauses := query_builder.OrClauses{}
	for _, role := range query.GetRole() {
		roleClauses.AddOr("role = ?", role)
	}
	if roleQuery, roleArgs := roleClauses.Build(); roleQuery != "" {
		q.AddWhereClause("("+roleQuery+")", roleArgs...)
	}
	if start := query.GetUpdatedAfter(); start.IsValid() {
		q.AddWhereClause("updated_at_usec >= ?", start.AsTime().UnixMicro())
	}
	if end := query.GetUpdatedBefore(); end.IsValid() {
		q.AddWhereClause("updated_at_usec < ?", end.AsTime().UnixMicro())
	}
	if tags := query.GetTags(); len(tags) > 0 {
		clause, args := invocation_format.GetTagsAsClickhouseWhereClause("tags", tags)
		q.AddWhereClause(clause, args...)
	}

	statusClauses := query_builder.OrClauses{}
	for _, status := range query.GetInvocationStatus() {
		switch status {
		case ispb.OverallStatus_SUCCESS:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS), 1)
		case ispb.OverallStatus_FAILURE:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(ispb.InvocationStatus_COMPLETE_INVOCATION_STATUS), 0)
		case ispb.OverallStatus_IN_PROGRESS:
			statusClauses.AddOr(`invocation_status = ?`, int(ispb.InvocationStatus_PARTIAL_INVOCATION_STATUS))
		case ispb.OverallStatus_DISCONNECTED:
			statusClauses.AddOr(`invocation_status = ?`, int(ispb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS))
		case ispb.OverallStatus_UNKNOWN_OVERALL_STATUS:
			continue
		default:
			continue
		}
	}
	if statusQuery, statusArgs := statusClauses.Build(); statusQuery != "" {
		q.AddWhereClause(fmt.Sprintf("(%s)", statusQuery), statusArgs...)
	}

	for _, f := range query.GetFilter() {
		if f.GetMetric().Execution == nil {
			continue
		}
		str, args, err := filter.GenerateFilterStringAndArgs(f)
		if err != nil {
			return err
		}
		q.AddWhereClause(str, args...)
	}
	for _, f := range query.GetDimensionFilter() {
		str, args, err := filter.GenerateDimensionFilterStringAndArgs(f)
		if err != nil {
			return err
		}
		q.AddWhereClause(str, args...)
	}
	for _, f := range query.GetGenericFilters() {
		str, args, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(f, stat_filter.ObjectTypes_EXECUTION_OBJECTS, s.oh.DialectName())
		if err != nil {
			return err
		}
		q.AddWhereClause(str, args...)
	}
	return nil
}

// executionTimelineInterval returns the stats bucket size and timezone to use
// for a timeline covering the time range in `query`, mirroring the interval
// selection that invocation_stat_service performs for the GetTrend RPC: the
// bucket size is chosen to keep the response under ~50 intervals for the
// queried date range, falling back to 1-day buckets when finer time buckets
// are disabled.
func executionTimelineInterval(query *expb.ExecutionQuery, timezone string) (invocation_stat_service.StatInterval, *time.Location) {
	endTime := time.Now()
	if end := query.GetUpdatedBefore(); end.IsValid() {
		endTime = end.AsTime()
	}
	startTime := endTime.Add(-invocation_stat_service.ONE_WEEK)
	if start := query.GetUpdatedAfter(); start.IsValid() {
		startTime = start.AsTime()
	}

	location, err := time.LoadLocation(timezone)
	if err != nil || location.String() == time.Local.String() {
		location = time.UTC
	}

	interval := invocation_stat_service.StatInterval1Day
	if invocation_stat_service.FinerTimeBucketsEnabled() {
		interval = invocation_stat_service.ComputeTrendsInterval(endTime.Sub(startTime))
	}
	return interval, location
}

// addTimelineWhereClauses applies the WHERE clauses shared by both
// GetExecutionTimeline queries (the stats aggregation and the execution
// sample).
func (s *ExecutionSearchService) addTimelineWhereClauses(q *query_builder.Query, groupID string, req *expb.GetExecutionTimelineRequest) error {
	// Always filter to the currently selected (and authorized) group, and to
	// the requested target to constrain the scan size.
	q.AddWhereClause("group_id = ?", groupID)
	q.AddWhereClause("target_label = ?", req.GetTarget())
	// Only include executions that actually ran on a worker; entries that never
	// started have nothing meaningful to plot on the timeline.
	q.AddWhereClause("worker_start_timestamp_usec > 0")
	q.AddWhereClause("worker_completed_timestamp_usec > 0")
	return s.addExecutionQueryFilters(q, req.GetQuery())
}

// timelineStatsRow is a single row of the OLAP aggregation query issued by
// GetExecutionTimeline: a summary of one timeline's executions, either within
// a single time bucket (bucket_start_time_usec > 0) or across the whole
// timeline (bucket_start_time_usec == 0, from the GROUPING SETS rollup).
type timelineStatsRow struct {
	CleanedOutputPath   string
	ActionMnemonic      string
	OS                  string
	Arch                string
	BucketStartTimeUsec int64
	DurationUsecTotal   int64
	DurationUsecP50     int64
	DurationUsecP90     int64
	CPUNanosTotal       int64
	CPUNanosP50         int64
	CPUNanosP90         int64
	PeakMemoryP50       int64
	PeakMemoryP90       int64
}

func (r *timelineStatsRow) toSummaryProto() *expb.ExecutionTimelineSummary {
	return &expb.ExecutionTimelineSummary{
		DurationUsecTotal: r.DurationUsecTotal,
		DurationUsecP50:   r.DurationUsecP50,
		DurationUsecP90:   r.DurationUsecP90,
		CpuNanosTotal:     r.CPUNanosTotal,
		CpuNanosP50:       r.CPUNanosP50,
		CpuNanosP90:       r.CPUNanosP90,
		PeakMemoryP50:     r.PeakMemoryP50,
		PeakMemoryP90:     r.PeakMemoryP90,
	}
}

// timelineKey identifies the timeline an execution belongs to: executions
// with the same (run-stripped) output path, mnemonic, os, and arch are
// plotted together.
func timelineKey(cleanedOutputPath, mnemonic, os, arch string) string {
	return cleanedOutputPath + "|" + mnemonic + "|" + os + "|" + arch
}

// queryTimelineStats computes summary stats for every timeline matching the
// request directly in the OLAP DB.  It returns one row per (timeline, time
// bucket) pair plus one whole-timeline rollup row per timeline (with
// bucket_start_time_usec == 0), ordered so that each timeline's rollup row
// immediately precedes its bucket rows.
//
// quantilesExactLow is used because it matches the nearest-rank percentiles
// this service previously computed in Go (including returning the lower of
// the two middle values for the median of an even-sized set).
func (s *ExecutionSearchService) queryTimelineStats(ctx context.Context, req *expb.GetExecutionTimelineRequest, groupID string, interval invocation_stat_service.StatInterval, location *time.Location) ([]*timelineStatsRow, error) {
	bucketExpr, bucketArgs := s.oh.BucketFromUsecTimestamp("worker_start_timestamp_usec", location, interval.ClickhouseInterval())
	q := query_builder.NewQueryWithArgs(`
		SELECT
			replaceRegexpAll(output_path, ?, '') AS cleaned_output_path,
			action_mnemonic,
			os,
			arch,
			`+bucketExpr+` AS bucket_start_time_usec,
			SUM(worker_completed_timestamp_usec - worker_start_timestamp_usec) AS duration_usec_total,
			SUM(cpu_nanos) AS cpu_nanos_total,
			arrayElement(quantilesExactLow(0.5, 0.9)(worker_completed_timestamp_usec - worker_start_timestamp_usec), 1) AS duration_usec_p50,
			arrayElement(quantilesExactLow(0.5, 0.9)(worker_completed_timestamp_usec - worker_start_timestamp_usec), 2) AS duration_usec_p90,
			arrayElement(quantilesExactLow(0.5, 0.9)(cpu_nanos), 1) AS cpu_nanos_p50,
			arrayElement(quantilesExactLow(0.5, 0.9)(cpu_nanos), 2) AS cpu_nanos_p90,
			arrayElement(quantilesExactLow(0.5, 0.9)(peak_memory_bytes), 1) AS peak_memory_p50,
			arrayElement(quantilesExactLow(0.5, 0.9)(peak_memory_bytes), 2) AS peak_memory_p90
		FROM "Executions"
	`, append([]interface{}{runMatcher.String()}, bucketArgs...))

	if err := s.addTimelineWhereClauses(q, groupID, req); err != nil {
		return nil, err
	}

	// GROUPING SETS gives us both per-bucket rows and a whole-timeline rollup
	// row in a single scan; the rollup rows get the default value (0) for
	// bucket_start_time_usec, which cannot collide with a real bucket because
	// we exclude executions with worker_start_timestamp_usec == 0 above.
	q.SetGroupBy("GROUPING SETS ((cleaned_output_path, action_mnemonic, os, arch, bucket_start_time_usec), (cleaned_output_path, action_mnemonic, os, arch))")
	q.SetOrderBy("cleaned_output_path, action_mnemonic, os, arch, bucket_start_time_usec", true)

	qString, qArgs := q.Build()
	rq := s.oh.NewQuery(ctx, "execution_search_service_timeline_stats").Raw(qString, qArgs...)
	return db.ScanAll(rq, &timelineStatsRow{})
}

// queryTimelineExecutions fetches a uniformly random sample of up to
// timelineExecutionSampleSize individual executions matching the request, for
// rendering individual points on the timeline.
func (s *ExecutionSearchService) queryTimelineExecutions(ctx context.Context, req *expb.GetExecutionTimelineRequest, groupID string) ([]*schema.Execution, error) {
	q := query_builder.NewQuery(`
		SELECT worker_start_timestamp_usec, worker_completed_timestamp_usec, cpu_nanos, peak_memory_bytes, action_mnemonic, os, arch, output_path
		FROM "Executions"
	`)
	if err := s.addTimelineWhereClauses(q, groupID, req); err != nil {
		return nil, err
	}
	q.SetOrderBy("rand()", true)
	q.SetLimit(timelineExecutionSampleSize)

	qString, qArgs := q.Build()
	return s.rawQueryExecutions(ctx, qString, qArgs...)
}

// shardFromOutputPath extracts the shard number from a test output path like
// ".../shard_3_of_5/...", returning 0 if the path has no shard component.
func shardFromOutputPath(outputPath string) int64 {
	shardMatch := shardMatcher.FindStringSubmatch(outputPath)
	if len(shardMatch) > 1 {
		if shard, err := strconv.Atoi(shardMatch[1]); err == nil {
			return int64(shard)
		}
	}
	return 0
}

func (s *ExecutionSearchService) GetExecutionTimeline(ctx context.Context, req *expb.GetExecutionTimelineRequest) (*expb.GetExecutionTimelineResponse, error) {
	if s.oh == nil {
		return nil, status.UnavailableError("An OLAP DB is required to search executions.")
	}
	if req.GetTarget() == "" {
		return nil, status.InvalidArgumentError("A target is required to fetch an execution timeline.")
	}
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InvalidArgumentError("Failed to find user's group when searching executions.")
	}
	if err := authutil.AuthorizeGroupAccessForStats(ctx, s.env, u.GetGroupID()); err != nil {
		return nil, err
	}

	interval, location := executionTimelineInterval(req.GetQuery(), req.GetRequestContext().GetTimezone())

	statsRows, err := s.queryTimelineStats(ctx, req, u.GetGroupID(), interval, location)
	if err != nil {
		return nil, err
	}
	sampledExecutions, err := s.queryTimelineExecutions(ctx, req, u.GetGroupID())
	if err != nil {
		return nil, err
	}

	rsp := &expb.GetExecutionTimelineResponse{
		Interval: interval.IntervalProto(),
	}
	timelinesByKey := make(map[string]*expb.ExecutionTimeline)
	for _, row := range statsRows {
		k := timelineKey(row.CleanedOutputPath, row.ActionMnemonic, row.OS, row.Arch)
		if row.BucketStartTimeUsec == 0 {
			// Whole-timeline rollup row: starts a new timeline.
			tl := &expb.ExecutionTimeline{
				OutputPath: row.CleanedOutputPath,
				Mnemonic:   row.ActionMnemonic,
				Os:         row.OS,
				Arch:       row.Arch,
				Shard:      shardFromOutputPath(row.CleanedOutputPath),
				Summary:    row.toSummaryProto(),
			}
			timelinesByKey[k] = tl
			rsp.Timelines = append(rsp.Timelines, tl)
			continue
		}
		tl := timelinesByKey[k]
		if tl == nil {
			// Shouldn't happen: rollup rows sort before their bucket rows.
			continue
		}
		tl.AggregatedStats = append(tl.AggregatedStats, &expb.AggregatedExecutionTimelineEntry{
			BucketStartTimeUsec: row.BucketStartTimeUsec,
			Summary:             row.toSummaryProto(),
		})
	}
	for _, ex := range sampledExecutions {
		cleanedOutput := runMatcher.ReplaceAllString(ex.OutputPath, "")
		tl := timelinesByKey[timelineKey(cleanedOutput, ex.ActionMnemonic, ex.OS, ex.Arch)]
		if tl == nil {
			// The sample runs as a separate query from the stats, so a
			// just-written execution can miss its timeline; skip it.
			continue
		}
		tl.Execution = append(tl.Execution, &expb.ExecutionTimelineEntry{
			StartTimeUsec:   ex.WorkerStartTimestampUsec,
			DurationUsec:    ex.WorkerCompletedTimestampUsec - ex.WorkerStartTimestampUsec,
			CpuNanos:        ex.CPUNanos,
			PeakMemoryBytes: ex.PeakMemoryBytes,
		})
	}
	for _, tl := range rsp.Timelines {
		sort.Slice(tl.Execution, func(i, j int) bool {
			return tl.Execution[i].GetStartTimeUsec() < tl.Execution[j].GetStartTimeUsec()
		})
	}
	return rsp, nil
}

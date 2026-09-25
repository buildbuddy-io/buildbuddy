package execution_search_service

import (
	"context"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/stats"
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
	"golang.org/x/sync/errgroup"

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
	// Used to count run_35_of_200, etc. in the same timeline.
	runMatcher      = regexp.MustCompile(`[_/]run_\d+_of_\d+`)
	shardMatcher    = regexp.MustCompile(`/shard_(\d+)_of_\d+/`)
	quantiles       = []int32{0, 5, 10, 50, 90, 95, 100}
	quantileQString = "0, 0.05, 0.1, 0.5, 0.9, 0.95, 1"
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

func (s *ExecutionSearchService) rawQueryExecutions(ctx context.Context, query string, queryArgs ...any) ([]*schema.Execution, error) {
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
	} else {
		// If no start time is specified, default to 7 days.
		lookbackWindowHours := 7 * 24 * time.Hour
		q.AddWhereClause("updated_at_usec >= ?", time.Now().Add(-lookbackWindowHours).UnixMicro())
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
func executionTimelineInterval(query *expb.ExecutionQuery, timezone string, finerTimeBuckets bool) (stats.StatInterval, *time.Location) {
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

	interval := stats.StatInterval1Day
	if finerTimeBuckets {
		interval = stats.ComputeStatInterval(endTime.Sub(startTime))
	}
	return interval, location
}

func (s *ExecutionSearchService) addTimelineWhereClauses(q *query_builder.Query, groupID string, req *expb.GetExecutionTimelineRequest) error {
	q.AddWhereClause("group_id = ?", groupID)
	q.AddWhereClause("target_label = ?", req.GetTarget())
	// Only include executions that actually recorded a start and end time.
	q.AddWhereClause("worker_start_timestamp_usec > 0")
	q.AddWhereClause("worker_completed_timestamp_usec > 0")
	return s.addExecutionQueryFilters(q, req.GetQuery())
}

/** A timelineStatsRow is the GORM-friendly version of  `expb.ExecutionTimelineSummary` */
type timelineStatsRow struct {
	CleanedOutputPath      string
	ActionMnemonic         string
	OS                     string
	Arch                   string
	BucketStartTimeUsec    int64
	DurationUsecTotal      int64
	CPUNanosTotal          int64
	DownloadedBytesTotal   int64
	UploadedBytesTotal     int64
	WorkerQueueUsecTotal   int64
	InputDownloadUsecTotal int64
	ExecutionUsecTotal     int64
	OutputUploadUsecTotal  int64

	// Each of these is populated with an array of p10,p50,p90.
	DurationUsecQuantiles      []int64 `gorm:"type:int64[]"`
	CPUNanosQuantiles          []int64 `gorm:"type:int64[]"`
	PeakMemoryQuantiles        []int64 `gorm:"type:int64[]"`
	DownloadedBytesQuantiles   []int64 `gorm:"type:int64[]"`
	UploadedBytesQuantiles     []int64 `gorm:"type:int64[]"`
	WorkerQueueUsecQuantiles   []int64 `gorm:"type:int64[]"`
	InputDownloadUsecQuantiles []int64 `gorm:"type:int64[]"`
	ExecutionUsecQuantiles     []int64 `gorm:"type:int64[]"`
	OutputUploadUsecQuantiles  []int64 `gorm:"type:int64[]"`
}

func makeQuantiles(in []int64) []*expb.Quantile {
	out := make([]*expb.Quantile, 0, len(in))
	for i, q := range quantiles {
		// Current usage guarantees that these array lookups will always succeed.
		out = append(out, &expb.Quantile{Quantile: q, Value: in[i]})
	}
	return out
}

func (r *timelineStatsRow) toSummaryProto() *expb.ExecutionTimelineSummary {
	return &expb.ExecutionTimelineSummary{
		DurationUsecTotal:      r.DurationUsecTotal,
		DurationUsec:           makeQuantiles(r.DurationUsecQuantiles),
		CpuNanosTotal:          r.CPUNanosTotal,
		CpuNanos:               makeQuantiles(r.CPUNanosQuantiles),
		PeakMemory:             makeQuantiles(r.PeakMemoryQuantiles),
		DownloadedBytesTotal:   r.DownloadedBytesTotal,
		DownloadedBytes:        makeQuantiles(r.DownloadedBytesQuantiles),
		UploadedBytesTotal:     r.UploadedBytesTotal,
		UploadedBytes:          makeQuantiles(r.UploadedBytesQuantiles),
		WorkerQueueUsecTotal:   r.WorkerQueueUsecTotal,
		WorkerQueueUsec:        makeQuantiles(r.WorkerQueueUsecQuantiles),
		InputDownloadUsecTotal: r.InputDownloadUsecTotal,
		InputDownloadUsec:      makeQuantiles(r.InputDownloadUsecQuantiles),
		OutputUploadUsecTotal:  r.OutputUploadUsecTotal,
		OutputUploadUsec:       makeQuantiles(r.OutputUploadUsecQuantiles),
		ExecutionUsecTotal:     r.ExecutionUsecTotal,
		ExecutionUsec:          makeQuantiles(r.ExecutionUsecQuantiles),
	}
}

// timelineKey identifies the timeline an execution belongs to: executions
// with the same (run-stripped) output path, mnemonic, os, and arch are
// plotted together.
func timelineKey(cleanedOutputPath, mnemonic, os, arch string) string {
	return cleanedOutputPath + "|" + mnemonic + "|" + os + "|" + arch
}

/**
 * queryTimelineStats computes a timeline matching the provided filters directly
 * inside of ClickHouse.
 *
 * Timelines are keyed by a (trimmed_output_path, action_mnemonic, os, arch) tuple.
 * This returns one row per (tuple, time bucket) pair plus one rollup row per tuple
 * (identified with bucket_start_time_usec == 0), The rows are ordered so that each
 * timeline's rollup row immediately precedes its bucket rows.
 */
func (s *ExecutionSearchService) queryTimelineStats(ctx context.Context, req *expb.GetExecutionTimelineRequest, groupID string, interval stats.StatInterval, location *time.Location) ([]*timelineStatsRow, error) {
	durationUsec, err := filter.ExecutionMetricToDbField(stat_filter.ExecutionMetricType_EXECUTION_WALL_TIME_EXECUTION_METRIC)
	if err != nil {
		return nil, err
	}
	queuedUsec, err := filter.ExecutionMetricToDbField(stat_filter.ExecutionMetricType_QUEUE_TIME_USEC_EXECUTION_METRIC)
	if err != nil {
		return nil, err
	}
	inputDownloadUsec, err := filter.ExecutionMetricToDbField(stat_filter.ExecutionMetricType_INPUT_DOWNLOAD_TIME_EXECUTION_METRIC)
	if err != nil {
		return nil, err
	}
	executionUsec, err := filter.ExecutionMetricToDbField(stat_filter.ExecutionMetricType_REAL_EXECUTION_TIME_EXECUTION_METRIC)
	if err != nil {
		return nil, err
	}
	outputUploadUsec, err := filter.ExecutionMetricToDbField(stat_filter.ExecutionMetricType_OUTPUT_UPLOAD_TIME_EXECUTION_METRIC)
	if err != nil {
		return nil, err
	}
	bucketExpr, bucketArgs := s.oh.BucketFromUsecTimestamp("worker_start_timestamp_usec", location, interval.ClickhouseInterval())
	q := query_builder.NewQueryWithArgs(`
		SELECT
			replaceRegexpAll(output_path, ?, '') AS cleaned_output_path,
			action_mnemonic,
			os,
			arch,
			`+bucketExpr+` AS bucket_start_time_usec,
			SUM(`+durationUsec+`) AS duration_usec_total,
			SUM(`+queuedUsec+`) AS worker_queue_usec_total,
			SUM(`+inputDownloadUsec+`) AS input_download_usec_total,
			SUM(`+executionUsec+`) AS execution_usec_total,
			SUM(`+outputUploadUsec+`) AS output_upload_usec_total,
			SUM(cpu_nanos) AS cpu_nanos_total,
			SUM(file_download_size_bytes) AS downloaded_bytes_total,
			SUM(file_upload_size_bytes) AS uploaded_bytes_total,
			quantilesExactLow(`+quantileQString+`)(`+durationUsec+`) AS duration_usec_quantiles,
			quantilesExactLow(`+quantileQString+`)(`+queuedUsec+`) AS worker_queue_usec_quantiles,
			quantilesExactLow(`+quantileQString+`)(`+inputDownloadUsec+`) AS input_download_usec_quantiles,
			quantilesExactLow(`+quantileQString+`)(`+executionUsec+`) AS execution_usec_quantiles,
			quantilesExactLow(`+quantileQString+`)(`+outputUploadUsec+`) AS output_upload_usec_quantiles,
			quantilesExactLow(`+quantileQString+`)(cpu_nanos) AS cpu_nanos_quantiles,
			quantilesExactLow(`+quantileQString+`)(peak_memory_bytes) AS peak_memory_quantiles,
			quantilesExactLow(`+quantileQString+`)(file_download_size_bytes) AS downloaded_bytes_quantiles,
			quantilesExactLow(`+quantileQString+`)(file_upload_size_bytes) AS uploaded_bytes_quantiles
		FROM "Executions"
	`, append([]any{runMatcher.String()}, bucketArgs...))

	if err := s.addTimelineWhereClauses(q, groupID, req); err != nil {
		return nil, err
	}

	q.SetGroupBy("GROUPING SETS ((cleaned_output_path, action_mnemonic, os, arch, bucket_start_time_usec), (cleaned_output_path, action_mnemonic, os, arch))")
	q.SetOrderBy("cleaned_output_path, action_mnemonic, os, arch, bucket_start_time_usec", true)

	qString, qArgs := q.Build()
	rq := s.oh.NewQuery(ctx, "execution_search_service_timeline_stats").Raw(qString, qArgs...)
	return db.ScanAll(rq, &timelineStatsRow{})
}

/**
 * queryTimelineExecutions fetches a uniform random sample of executions matching
 * the user's query.  These values are used to show a representative sample of
 * executions to the user which they can then directly select for comparison.
 */
func (s *ExecutionSearchService) queryTimelineExecutions(ctx context.Context, req *expb.GetExecutionTimelineRequest, groupID string) ([]*schema.Execution, error) {
	q := query_builder.NewQuery(`
		SELECT invocation_uuid, action_digest_hash, queued_timestamp_usec, input_fetch_start_timestamp_usec, input_fetch_completed_timestamp_usec, execution_start_timestamp_usec, execution_completed_timestamp_usec, output_upload_start_timestamp_usec, output_upload_completed_timestamp_usec, worker_start_timestamp_usec, worker_completed_timestamp_usec, cpu_nanos, peak_memory_bytes, file_download_size_bytes, file_upload_size_bytes, action_mnemonic, os, arch, output_path
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

func shardFromOutputPath(outputPath string) int64 {
	shardMatch := shardMatcher.FindStringSubmatch(outputPath)
	if len(shardMatch) > 1 {
		if shard, err := strconv.Atoi(shardMatch[1]); err == nil {
			return int64(shard)
		}
	}
	return 0
}

func clampedDuration(start int64, end int64) int64 {
	if end < start || start == 0 {
		return 0
	}
	return end - start
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

	interval, location := executionTimelineInterval(req.GetQuery(), req.GetRequestContext().GetTimezone(), stats.FinerTimeBucketsEnabled())

	var statsRows []*timelineStatsRow
	var sampledExecutions []*schema.Execution
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		rows, err := s.queryTimelineStats(egCtx, req, u.GetGroupID(), interval, location)
		if err != nil {
			return err
		}
		statsRows = rows
		return nil
	})
	eg.Go(func() error {
		executions, err := s.queryTimelineExecutions(egCtx, req, u.GetGroupID())
		if err != nil {
			return err
		}
		sampledExecutions = executions
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	rsp := &expb.GetExecutionTimelineResponse{
		Interval: interval.IntervalProto(),
	}
	timelinesByKey := make(map[string]*expb.ExecutionTimeline)
	for _, row := range statsRows {
		k := timelineKey(row.CleanedOutputPath, row.ActionMnemonic, row.OS, row.Arch)

		// Timeline query data is sorted such that a summary row (start time == 0)
		// always comes before the corresponding data for that timeline.
		if row.BucketStartTimeUsec == 0 {
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
		// This shouldn't happen because the row.BucketStartTimeUsec == 0 block
		// above should always run before we process any other rows with the key `k`.
		if tl == nil {
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
		// Sampling runs as a separate query from timeline stats collection, so a
		// sampled execution might not match any timeline.  If this happens, skip it.
		if tl == nil {
			continue
		}
		id, err := uuid.Base64StringToString(ex.InvocationUUID)
		if err != nil {
			return nil, err
		}
		tl.Execution = append(tl.Execution, &expb.ExecutionTimelineEntry{
			ActionDigestHash:  hex.EncodeToString([]byte(ex.ActionDigestHash)),
			InvocationId:      id,
			StartTimeUsec:     ex.WorkerStartTimestampUsec,
			DurationUsec:      clampedDuration(ex.QueuedTimestampUsec, ex.WorkerCompletedTimestampUsec),
			CpuNanos:          ex.CPUNanos,
			WorkerQueueUsec:   clampedDuration(ex.QueuedTimestampUsec, ex.WorkerStartTimestampUsec),
			InputDownloadUsec: clampedDuration(ex.InputFetchStartTimestampUsec, ex.InputFetchCompletedTimestampUsec),
			ExecutionUsec:     clampedDuration(ex.ExecutionStartTimestampUsec, ex.ExecutionCompletedTimestampUsec),
			OutputUploadUsec:  clampedDuration(ex.OutputUploadStartTimestampUsec, ex.OutputUploadCompletedTimestampUsec),
			PeakMemoryBytes:   ex.PeakMemoryBytes,
			UploadedBytes:     ex.FileUploadSizeBytes,
			DownloadedBytes:   ex.FileDownloadSizeBytes,
		})
	}
	for _, tl := range rsp.Timelines {
		sort.Slice(tl.Execution, func(i, j int) bool {
			return tl.Execution[i].GetStartTimeUsec() < tl.Execution[j].GetStartTimeUsec()
		})
	}
	return rsp, nil
}

package invocation_stat_service

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"sort"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	stpb "github.com/buildbuddy-io/buildbuddy/proto/stats"
)

var (
	readFromOLAPDBEnabled        = flag.Bool("app.enable_read_from_olap_db", false, "If enabled, read from OLAP DB")
	executionTrendsEnabled       = flag.Bool("app.enable_execution_trends", false, "If enabled, fill execution trend stats in GetTrendResponse")
	invocationPercentilesEnabled = flag.Bool("app.enable_invocation_stat_percentiles", false, "If enabled, provide percentile breakdowns for invocation stats in GetTrendResponse")
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

func (i *InvocationStatService) getAggColumn(reqCtx *ctxpb.RequestContext, aggType inpb.AggType) string {
	switch aggType {
	case inpb.AggType_USER_AGGREGATION_TYPE:
		return "user"
	case inpb.AggType_HOSTNAME_AGGREGATION_TYPE:
		return "host"
	case inpb.AggType_GROUP_ID_AGGREGATION_TYPE:
		return "group_id"
	case inpb.AggType_REPO_URL_AGGREGATION_TYPE:
		return "repo_url"
	case inpb.AggType_COMMIT_SHA_AGGREGATION_TYPE:
		return "commit_sha"
	case inpb.AggType_DATE_AGGREGATION_TYPE:
		return i.dbh.DateFromUsecTimestamp("updated_at_usec", reqCtx.GetTimezoneOffsetMinutes())
	case inpb.AggType_BRANCH_AGGREGATION_TYPE:
		return "branch_name"
	case inpb.AggType_PATTERN_AGGREGATION_TYPE:
		return "pattern"
	default:
		log.Errorf("Unknown or unsupported aggregation column type: %s", aggType)
		return ""
	}
}

func (i *InvocationStatService) getTrendBasicQuery(timezoneOffsetMinutes int32) string {
	q := ""
	if i.isOLAPDBEnabled() {
		q = fmt.Sprintf("SELECT %s as name,", i.olapdbh.DateFromUsecTimestamp("updated_at_usec", timezoneOffsetMinutes)) + `
	    SUM(duration_usec) as total_build_time_usec,`
	} else {
		q = fmt.Sprintf("SELECT %s as name,", i.dbh.DateFromUsecTimestamp("updated_at_usec", timezoneOffsetMinutes)) + `
	    SUM(CASE WHEN duration_usec > 0 THEN duration_usec END) as total_build_time_usec,`
	}

	// Insert quantiles stuff..
	if i.isInvocationPercentilesEnabled() {
		q = q + `quantilesExactExclusive(0.5, 0.75, 0.9, 0.95, 0.99)(
				IF(duration_usec > 0, duration_usec, 0)) AS build_time_quantiles,`
	}

	q = q + `
	    COUNT(1) AS total_num_builds,
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
        SUM(total_upload_usec) as total_upload_usec
        FROM Invocations`
	return q
}

func flattenTrendsQuery(innerQuery string) string {
	return `SELECT name,
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
	arrayElement(build_time_quantiles, 1) as build_time_usec_p50,
	arrayElement(build_time_quantiles, 2) as build_time_usec_p75,
	arrayElement(build_time_quantiles, 3) as build_time_usec_p90,
	arrayElement(build_time_quantiles, 4) as build_time_usec_p95,
	arrayElement(build_time_quantiles, 5) as build_time_usec_p99
	FROM (` + innerQuery + ")"
}

func addWhereClauses(q *query_builder.Query, req *stpb.GetTrendRequest) error {
	groupID := req.GetRequestContext().GetGroupId()

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
	} else {
		// If no start time specified, respect the lookback window field if set,
		// or default to 7 days.
		// TODO(bduffany): Delete this once clients no longer need it.
		lookbackWindowDays := 7 * 24 * time.Hour
		if w := req.GetLookbackWindowDays(); w != 0 {
			if w < 1 || w > 365 {
				return status.InvalidArgumentErrorf("lookback_window_days must be between 0 and 366")
			}
			lookbackWindowDays = time.Duration(w*24) * time.Hour
		}
		q.AddWhereClause("updated_at_usec >= ?", time.Now().Add(-lookbackWindowDays).UnixMicro())
	}

	if end := req.GetQuery().GetUpdatedBefore(); end.IsValid() {
		q.AddWhereClause("updated_at_usec < ?", end.AsTime().UnixMicro())
	}

	statusClauses := toStatusClauses(req.GetQuery().GetStatus())
	statusQuery, statusArgs := statusClauses.Build()
	if statusQuery != "" {
		q.AddWhereClause(fmt.Sprintf("(%s)", statusQuery), statusArgs...)
	}

	q.AddWhereClause(`group_id = ?`, groupID)
	q.SetGroupBy("name")
	return nil
}

func (i *InvocationStatService) getInvocationTrend(ctx context.Context, req *stpb.GetTrendRequest) ([]*stpb.TrendStat, error) {
	reqCtx := req.GetRequestContext()

	q := query_builder.NewQuery(i.getTrendBasicQuery(reqCtx.GetTimezoneOffsetMinutes()))
	if err := addWhereClauses(q, req); err != nil {
		return nil, err
	}

	qStr, qArgs := q.Build()
	if i.isInvocationPercentilesEnabled() {
		qStr = flattenTrendsQuery(qStr)
	}

	var rows *sql.Rows
	var err error
	if i.isOLAPDBEnabled() {
		rows, err = i.olapdbh.DB(ctx).Raw(qStr, qArgs...).Rows()
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

func (i *InvocationStatService) getExecutionTrendQuery(timezoneOffsetMinutes int32) string {
	return fmt.Sprintf("SELECT %s as name,", i.olapdbh.DateFromUsecTimestamp("updated_at_usec", timezoneOffsetMinutes)) + `
	quantilesExactExclusive(0.5, 0.75, 0.9, 0.95, 0.99)(IF(worker_start_timestamp_usec > queued_timestamp_usec, worker_start_timestamp_usec - queued_timestamp_usec, 0)) AS queue_duration_usec_quantiles
	FROM Executions
	`
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
	return `SELECT name, 
	arrayElement(queue_duration_usec_quantiles, 1) as queue_duration_usec_p50,
	arrayElement(queue_duration_usec_quantiles, 2) as queue_duration_usec_p75,
	arrayElement(queue_duration_usec_quantiles, 3) as queue_duration_usec_p90,
	arrayElement(queue_duration_usec_quantiles, 4) as queue_duration_usec_p95,
	arrayElement(queue_duration_usec_quantiles, 5) as queue_duration_usec_p99
	FROM (` + innerQuery + ")"
}

func (i *InvocationStatService) getExecutionTrend(ctx context.Context, req *stpb.GetTrendRequest) ([]*stpb.ExecutionStat, error) {
	if !i.isOLAPDBEnabled() || !*executionTrendsEnabled {
		return nil, nil
	}
	reqCtx := req.GetRequestContext()

	q := query_builder.NewQuery(i.getExecutionTrendQuery(reqCtx.GetTimezoneOffsetMinutes()))
	if err := addWhereClauses(q, req); err != nil {
		return nil, err
	}

	qStr, qArgs := q.Build()
	qStr = getQueryWithFlattenedArray(qStr)
	rows, err := i.olapdbh.DB(ctx).Raw(qStr, qArgs...).Rows()
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
	groupID := req.GetRequestContext().GetGroupId()
	if err := perms.AuthorizeGroupAccess(ctx, i.env, groupID); err != nil {
		return nil, err
	}
	if blocklist.IsBlockedForStatsQuery(groupID) {
		return nil, status.ResourceExhaustedErrorf("Too many rows.")
	}

	rsp := &stpb.GetTrendResponse{}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		var err error
		if rsp.TrendStat, err = i.getInvocationTrend(ctx, req); err != nil {
			return err
		}
		return nil
	})
	eg.Go(func() error {
		var err error
		if rsp.ExecutionStat, err = i.getExecutionTrend(ctx, req); err != nil {
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
            FROM Invocations`
	return q
}

func (i *InvocationStatService) GetInvocationStat(ctx context.Context, req *inpb.GetInvocationStatRequest) (*inpb.GetInvocationStatResponse, error) {
	if req.GetAggregationType() == inpb.AggType_UNKNOWN_AGGREGATION_TYPE {
		return nil, status.InvalidArgumentError("A valid aggregation type must be provided")
	}

	groupID := req.GetRequestContext().GetGroupId()
	if err := perms.AuthorizeGroupAccess(ctx, i.env, groupID); err != nil {
		return nil, err
	}
	if blocklist.IsBlockedForStatsQuery(groupID) {
		return nil, status.ResourceExhaustedErrorf("Too many rows.")
	}

	limit := int32(100)
	if l := req.GetLimit(); l != 0 {
		if l < 1 || l > 1000 {
			return nil, status.InvalidArgumentErrorf("limit must be between 0 and 1000")
		}
		limit = l
	}

	aggColumn := i.getAggColumn(req.GetRequestContext(), req.AggregationType)
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
	var err error
	if i.isOLAPDBEnabled() {
		rows, err = i.olapdbh.DB(ctx).Raw(qStr, qArgs...).Rows()
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

func toStatusClauses(statuses []inpb.OverallStatus) *query_builder.OrClauses {
	statusClauses := &query_builder.OrClauses{}
	for _, status := range statuses {
		switch status {
		case inpb.OverallStatus_SUCCESS:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(inpb.Invocation_COMPLETE_INVOCATION_STATUS), 1)
		case inpb.OverallStatus_FAILURE:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(inpb.Invocation_COMPLETE_INVOCATION_STATUS), 0)
		case inpb.OverallStatus_IN_PROGRESS:
			statusClauses.AddOr(`invocation_status = ?`, int(inpb.Invocation_PARTIAL_INVOCATION_STATUS))
		case inpb.OverallStatus_DISCONNECTED:
			statusClauses.AddOr(`invocation_status = ?`, int(inpb.Invocation_DISCONNECTED_INVOCATION_STATUS))
		case inpb.OverallStatus_UNKNOWN_OVERALL_STATUS:
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

func (i *InvocationStatService) isInvocationPercentilesEnabled() bool {
	return i.isOLAPDBEnabled() && *invocationPercentilesEnabled
}

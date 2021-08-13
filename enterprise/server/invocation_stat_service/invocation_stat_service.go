package invocation_stat_service

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type InvocationStatService struct {
	env environment.Env
	h   *db.DBHandle
}

func NewInvocationStatService(env environment.Env, h *db.DBHandle) *InvocationStatService {
	return &InvocationStatService{
		env: env,
		h:   h,
	}
}

func (i *InvocationStatService) getAggColumn(aggType inpb.AggType) string {
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
		return i.h.DateFromUsecTimestamp("updated_at_usec")
	default:
		log.Errorf("Unknown aggregation column type: %s", aggType)
		return ""
	}
}

func (i *InvocationStatService) GetTrend(ctx context.Context, req *inpb.GetTrendRequest) (*inpb.GetTrendResponse, error) {
	groupID := req.GetRequestContext().GetGroupId()
	if err := perms.AuthorizeGroupAccess(ctx, i.env, groupID); err != nil {
		return nil, err
	}
	if blocklist.IsBlockedForStatsQuery(groupID) {
		return nil, status.ResourceExhaustedErrorf("Too many rows.")
	}

	q := query_builder.NewQuery(fmt.Sprintf("SELECT %s as name,", i.h.DateFromUsecTimestamp("updated_at_usec")) + `
	    SUM(CASE WHEN duration_usec > 0 THEN duration_usec END) as total_build_time_usec,
	    COUNT(1) as total_num_builds,
	    SUM(CASE WHEN duration_usec > 0 THEN 1 ELSE 0 END) as completed_invocation_count,
	    COUNT(DISTINCT user) as user_count,
	    COUNT(DISTINCT commit_sha) as commit_count,
	    COUNT(DISTINCT host) as host_count,
	    COUNT(DISTINCT repo_url) as repo_count,
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
            FROM Invocations`)

	if user := req.GetQuery().GetUser(); user != "" {
		q.AddWhereClause("user = ?", user)
	}

	if host := req.GetQuery().GetHost(); host != "" {
		q.AddWhereClause("host = ?", host)
	}

	if repoURL := req.GetQuery().GetRepoUrl(); repoURL != "" {
		q.AddWhereClause("repo = ?", repoURL)
	}

	if commitSHA := req.GetQuery().GetCommitSha(); commitSHA != "" {
		q.AddWhereClause("commit = ?", commitSHA)
	}

	roleClauses := query_builder.OrClauses{}
	for _, role := range req.GetQuery().GetRole() {
		roleClauses.AddOr("role = ?", role)
	}
	if roleQuery, roleArgs := roleClauses.Build(); roleQuery != "" {
		q.AddWhereClause("("+roleQuery+")", roleArgs...)
	}

	if start := req.GetQuery().GetUpdatedAfter(); start.IsValid() {
		q.AddWhereClause("updated_at_usec >= ?", timeutil.ToUsec(start.AsTime()))
	} else {
		// If no start time specified, respect the lookback window field if set,
		// or default to 7 days.
		// TODO(bduffany): Delete this once clients no longer need it.
		lookbackWindowDays := 7 * 24 * time.Hour
		if w := req.GetLookbackWindowDays(); w != 0 {
			if w < 1 || w > 365 {
				return nil, status.InvalidArgumentErrorf("lookback_window_days must be between 0 and 366")
			}
			lookbackWindowDays = time.Duration(w*24) * time.Hour
		}
		q.AddWhereClause("updated_at_usec >= ?", timeutil.ToUsec(time.Now().Add(-lookbackWindowDays)))
	}

	if end := req.GetQuery().GetUpdatedBefore(); end.IsValid() {
		q.AddWhereClause("updated_at_usec < ?", timeutil.ToUsec(end.AsTime()))
	}

	statusClauses := toStatusClauses(req.GetQuery().GetStatus())
	statusQuery, statusArgs := statusClauses.Build()
	if statusQuery != "" {
		q.AddWhereClause(fmt.Sprintf("(%s)", statusQuery), statusArgs...)
	}

	q.AddWhereClause(`group_id = ?`, groupID)
	q.SetGroupBy("name")
	q.SetOrderBy("MAX(updated_at_usec)" /*ascending=*/, false)

	qStr, qArgs := q.Build()
	rows, err := i.h.Raw(qStr, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rsp := &inpb.GetTrendResponse{}
	rsp.TrendStat = make([]*inpb.TrendStat, 0)

	for rows.Next() {
		stat := &inpb.TrendStat{}
		if err := i.h.ScanRows(rows, &stat); err != nil {
			return nil, err
		}
		rsp.TrendStat = append(rsp.TrendStat, stat)
	}
	return rsp, nil
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

	aggColumn := i.getAggColumn(req.AggregationType)
	q := query_builder.NewQuery(fmt.Sprintf("SELECT %s as name,", aggColumn) + `
	    SUM(CASE WHEN duration_usec > 0 THEN duration_usec END) as total_build_time_usec,
	    MAX(updated_at_usec) as latest_build_time_usec,
	    MAX(CASE WHEN (success AND invocation_status = 1) THEN updated_at_usec END) as last_green_build_usec,
	    MAX(CASE WHEN (success != true AND invocation_status = 1) THEN updated_at_usec END) as last_red_build_usec,
	    COUNT(1) as total_num_builds,
	    COUNT(CASE WHEN (success AND invocation_status = 1) THEN 1 END) as total_num_sucessful_builds,
	    COUNT(CASE WHEN (success != true AND invocation_status = 1) THEN 1 END) as total_num_failing_builds,
	    SUM(action_count) as total_actions
            FROM Invocations`)

	if req.AggregationType != inpb.AggType_DATE_AGGREGATION_TYPE {
		q.AddWhereClause(`? != ""`, aggColumn)
	}

	if user := req.GetQuery().GetUser(); user != "" {
		q.AddWhereClause("user = ?", user)
	}

	if host := req.GetQuery().GetHost(); host != "" {
		q.AddWhereClause("host = ?", host)
	}

	if repoURL := req.GetQuery().GetRepoUrl(); repoURL != "" {
		q.AddWhereClause("repo = ?", repoURL)
	}

	if commitSHA := req.GetQuery().GetCommitSha(); commitSHA != "" {
		q.AddWhereClause("commit = ?", commitSHA)
	}

	roleClauses := query_builder.OrClauses{}
	for _, role := range req.GetQuery().GetRole() {
		roleClauses.AddOr("role = ?", role)
	}
	if roleQuery, roleArgs := roleClauses.Build(); roleQuery != "" {
		q.AddWhereClause("("+roleQuery+")", roleArgs...)
	}

	if start := req.GetQuery().GetUpdatedAfter(); start.IsValid() {
		q.AddWhereClause("updated_at_usec >= ?", timeutil.ToUsec(start.AsTime()))
	}

	if end := req.GetQuery().GetUpdatedBefore(); end.IsValid() {
		q.AddWhereClause("updated_at_usec < ?", timeutil.ToUsec(end.AsTime()))
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
	rows, err := i.h.Raw(qStr, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	rsp := &inpb.GetInvocationStatResponse{}
	rsp.InvocationStat = make([]*inpb.InvocationStat, 0)

	for rows.Next() {
		stat := &inpb.InvocationStat{}
		if err := i.h.ScanRows(rows, &stat); err != nil {
			return nil, err
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

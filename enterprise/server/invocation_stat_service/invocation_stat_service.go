package invocation_stat_service

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	requestcontext "github.com/buildbuddy-io/buildbuddy/server/util/request_context"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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
		return i.h.DateFromUsecTimestamp("created_at_usec")
	default:
		return ""
	}
}

// TODO(tylerw): Unify this function across handlers. It's duplicated (almost) in
// buildbuddy_server.go when generating the bazelrc config.
func (i *InvocationStatService) getGroupID(ctx context.Context) (string, error) {
	if reqCtx := requestcontext.ProtoRequestContextFromContext(ctx); reqCtx != nil {
		return reqCtx.GetGroupId(), nil
	}

	tu, err := i.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return "", err
	}
	for _, g := range tu.Groups {
		if g.OwnedDomain != "" || !i.env.GetConfigurator().GetAppNoDefaultUserGroup() {
			return g.GroupID, nil
		}
	}
	// Still here? This user might have a self-owned group, let's check for that.
	for _, g := range tu.Groups {
		if g.GroupID == strings.Replace(tu.UserID, "US", "GR", 1) {
			return g.GroupID, nil
		}
	}
	// Finally, fall back to any group with a write token
	for _, g := range tu.Groups {
		if g.WriteToken != "" {
			return g.GroupID, nil
		}
	}
	return "", status.FailedPreconditionError("User not in any searchable groups")
}

func (i *InvocationStatService) GetInvocationStat(ctx context.Context, req *inpb.GetInvocationStatRequest) (*inpb.GetInvocationStatResponse, error) {
	if req.GetAggregationType() == inpb.AggType_UNKNOWN_AGGREGATION_TYPE {
		return nil, status.InvalidArgumentError("A valid aggregation type must be provided")
	}

	// Check user was set, and then determine user's group_id.
	groupID, err := i.getGroupID(ctx)
	if err != nil {
		return nil, err
	}

	limit := int32(100)
	if requestLimit := req.Limit; requestLimit > 0 {
		limit = requestLimit
	}

	if limit > 365 || limit <= 0 {
		return nil, status.InvalidArgumentError("Limit must be between 1 and 365")
	}

	aggColumn := i.getAggColumn(req.AggregationType)

	filters := ""
	values := []interface{}{groupID}

	if user := req.GetQuery().GetUser(); user != "" {
		filters = filters + " AND user = ?"
		values = append(values, user)
	}

	if host := req.GetQuery().GetHost(); host != "" {
		filters = filters + " AND host = ?"
		values = append(values, host)
	}

	if repoURL := req.GetQuery().GetRepoUrl(); repoURL != "" {
		filters = filters + " AND repo_url = ?"
		values = append(values, repoURL)
	}

	if commitSHA := req.GetQuery().GetCommitSha(); commitSHA != "" {
		filters = filters + " AND commit_sha = ?"
		values = append(values, commitSHA)
	}

	if role := req.GetQuery().GetRole(); role != "" {
		filters = filters + " AND role = ?"
		values = append(values, role)
	}

	values = append(values, limit)

	q := "SELECT " + aggColumn + " as name," + `
                     SUM(CASE WHEN duration_usec > 0 THEN duration_usec END) as total_build_time_usec,
                     COUNT(DISTINCT invocation_id) as total_num_builds,
                     COUNT(DISTINCT CASE WHEN (success AND invocation_status = 1) THEN invocation_id END) as total_num_sucessful_builds,
                     COUNT(DISTINCT CASE WHEN (success != true AND invocation_status = 1) THEN invocation_id END) as total_num_failing_builds,
                     COUNT(DISTINCT CASE WHEN invocation_status != 1 THEN invocation_id END) as total_num_in_progress_builds,
                     MAX(updated_at_usec) as latest_build_time_usec,
                     SUM(action_count) as total_actions,
                     MAX(CASE WHEN (success AND invocation_status = 1) THEN updated_at_usec END) as last_green_build_usec,
                     MAX(CASE WHEN (success != true AND invocation_status = 1) THEN updated_at_usec END) as last_red_build_usec,
                     SUM(CASE WHEN command = 'build' THEN 1 ELSE 0 END) as build_command_count,
                     SUM(CASE WHEN command = 'test' THEN 1 ELSE 0 END) as test_command_count,
                     SUM(CASE WHEN command = 'run' THEN 1 ELSE 0 END) as run_command_count,
                     SUM(CASE WHEN duration_usec > 0 THEN 1 ELSE 0 END) as completed_invocation_count,

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

                     MAX(duration_usec) as max_duration_usec,
                     COUNT(DISTINCT user) as user_count,
                     COUNT(DISTINCT host) as host_count,
                     COUNT(DISTINCT commit_sha) as commit_count,
                     COUNT(DISTINCT repo_url) as repo_count
              FROM Invocations
              WHERE group_id = ?
              AND ` + aggColumn + ` != ""
              AND ` + aggColumn + ` IS NOT NULL
              ` + filters + `
              GROUP by name
							ORDER BY latest_build_time_usec DESC
							LIMIT ?`
	rows, err := i.h.Raw(q, values...).Rows()
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

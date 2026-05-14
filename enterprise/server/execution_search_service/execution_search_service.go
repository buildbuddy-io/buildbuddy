package execution_search_service

import (
	"context"
	"fmt"
	"strconv"
	"strings"

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

	if user := req.GetQuery().GetInvocationUser(); user != "" {
		q.AddWhereClause("\"user\" = ?", user)
	}
	if host := req.GetQuery().GetInvocationHost(); host != "" {
		q.AddWhereClause("host = ?", host)
	}
	if url := req.GetQuery().GetRepoUrl(); url != "" {
		q.AddWhereClause("repo_url = ?", url)
	}
	if branch := req.GetQuery().GetBranchName(); branch != "" {
		q.AddWhereClause("branch_name = ?", branch)
	}
	if command := req.GetQuery().GetCommand(); command != "" {
		q.AddWhereClause("command = ?", command)
	}
	if pattern := req.GetQuery().GetPattern(); pattern != "" {
		q.AddWhereClause("pattern = ?", pattern)
	}
	if sha := req.GetQuery().GetCommitSha(); sha != "" {
		q.AddWhereClause("commit_sha = ?", sha)
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
	if tags := req.GetQuery().GetTags(); len(tags) > 0 {
		clause, args := invocation_format.GetTagsAsClickhouseWhereClause("tags", tags)
		q.AddWhereClause(clause, args...)
	}

	statusClauses := query_builder.OrClauses{}
	for _, status := range req.GetQuery().GetInvocationStatus() {
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
	statusQuery, statusArgs := statusClauses.Build()
	if statusQuery != "" {
		q.AddWhereClause(fmt.Sprintf("(%s)", statusQuery), statusArgs...)
	}

	for _, f := range req.GetQuery().GetFilter() {
		if f.GetMetric().Execution == nil {
			continue
		}
		str, args, err := filter.GenerateFilterStringAndArgs(f)
		if err != nil {
			return nil, err
		}
		q.AddWhereClause(str, args...)
	}
	for _, f := range req.GetQuery().GetDimensionFilter() {
		str, args, err := filter.GenerateDimensionFilterStringAndArgs(f)
		if err != nil {
			return nil, err
		}
		q.AddWhereClause(str, args...)
	}

	for _, f := range req.GetQuery().GetGenericFilters() {
		s, a, err := filter.ValidateAndGenerateGenericFilterQueryStringAndArgs(f, stat_filter.ObjectTypes_EXECUTION_OBJECTS, s.oh.DialectName())
		if err != nil {
			return nil, err
		}
		q.AddWhereClause(s, a...)
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

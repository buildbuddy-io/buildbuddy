package execution_search_service

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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
	rows, err := s.oh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("search_executions"), query, queryArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	executions := make([]*schema.Execution, 0)
	for rows.Next() {
		var te schema.Execution
		if err := s.oh.DB(ctx).ScanRows(rows, &te); err != nil {
			return nil, err
		}
		executions = append(executions, &te)
	}
	return executions, nil
}

type ExecutionWithInvocationId struct {
	execution    *expb.Execution
	invocationID string
}

func (s *ExecutionSearchService) fetchExecutionData(ctx context.Context, groupId string, execIds []string) (map[string]*ExecutionWithInvocationId, error) {
	q := query_builder.NewQuery(`SELECT * FROM "Executions"`)
	q.AddWhereClause("execution_id IN ?", execIds)
	q.AddWhereClause("group_id = ?", groupId)
	if err := perms.AddPermissionsCheckToQuery(ctx, s.env, q); err != nil {
		return nil, err
	}
	qString, qArgs := q.Build()

	rows, err := s.h.RawWithOptions(ctx, db.Opts().WithQueryName("fetch_executions"), qString, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	executions := make(map[string]*ExecutionWithInvocationId, 0)
	for rows.Next() {
		var r tables.Execution
		if err := s.h.DB(ctx).ScanRows(rows, &r); err != nil {
			return nil, err
		}
		exec, err := execution.TableExecToClientProto(&r)
		if err != nil {
			return nil, err
		}
		executions[r.ExecutionID] = &ExecutionWithInvocationId{execution: exec, invocationID: r.InvocationID}
	}
	return executions, nil
}

func clickhouseExecutionToProto(in *schema.Execution, ex *ExecutionWithInvocationId) (*expb.ExecutionWithInvocationMetadata, error) {
	if in == nil || ex == nil {
		return nil, status.InternalErrorf("Execution not found or not accessible.")
	}
	return &expb.ExecutionWithInvocationMetadata{
		Execution: ex.execution,
		InvocationMetadata: &expb.InvocationMetadata{
			Id:               ex.invocationID,
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
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InvalidArgumentError("Failed to find user's group when searching executions.")
	}
	if err := perms.AuthorizeGroupAccessForStats(ctx, s.env, u.GetGroupID()); err != nil {
		return nil, err
	}

	q := query_builder.NewQuery(`SELECT * FROM "Executions"`)

	// Always filter to the currently selected (and authorized) group.
	q.AddWhereClause("group_id = ?", u.GetGroupID())

	if user := req.GetQuery().GetInvocationUser(); user != "" {
		q.AddWhereClause("user = ?", user)
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
		str, args, err := filter.GenerateFilterStringAndArgs(f, "")
		if err != nil {
			return nil, err
		}
		q.AddWhereClause(str, args...)
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
	tableExecutions, err := s.rawQueryExecutions(ctx, qString, qArgs...)
	if err != nil {
		return nil, err
	}
	execIds := make([]string, len(tableExecutions))
	for i, e := range tableExecutions {
		execIds[i] = e.ExecutionID
	}

	fullExecutions, err := s.fetchExecutionData(ctx, u.GetGroupID(), execIds)
	if err != nil {
		return nil, err
	}

	rsp := &expb.SearchExecutionResponse{}
	for _, te := range tableExecutions {
		ex, err := clickhouseExecutionToProto(te, fullExecutions[te.ExecutionID])
		if err != nil {
			return nil, err
		}
		rsp.Execution = append(rsp.Execution, ex)
	}
	if int64(len(rsp.Execution)) == limitSize {
		rsp.NextPageToken = pageSizeOffsetPrefix + strconv.FormatInt(offset+limitSize, 10)
	}
	return rsp, nil
}

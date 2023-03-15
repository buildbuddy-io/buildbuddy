package execution_search_service

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	expb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	ispb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

const (
	// See defaultSortParams() for sort defaults.
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

func tableExecToProto(in tables.Execution) (*expb.Execution, error) {
	r, err := digest.ParseDownloadResourceName(in.ExecutionID)
	if err != nil {
		return nil, err
	}

	var actionResultDigest *repb.Digest
	if in.StatusCode == int32(codes.OK) && in.ExitCode == 0 && !in.DoNotCache {
		// Action Result with unmodified action digest is only uploaded when
		// there is no error from the CommandResult(i.e. status code is OK) and
		// the exit code is zero and the action was not marked with DoNotCache.
		actionResultDigest = proto.Clone(r.GetDigest()).(*repb.Digest)
	} else {
		actionResultDigest, err = digest.AddInvocationIDToDigest(r.GetDigest(), in.InvocationID)
		if err != nil {
			return nil, err
		}
	}

	out := &expb.Execution{
		ActionDigest:       r.GetDigest(),
		ActionResultDigest: actionResultDigest,
		Status: &statuspb.Status{
			Code:    in.StatusCode,
			Message: in.StatusMessage,
		},
		ExitCode: in.ExitCode,
		Stage:    repb.ExecutionStage_Value(in.Stage),
		ExecutedActionMetadata: &repb.ExecutedActionMetadata{
			Worker:                         in.Worker,
			QueuedTimestamp:                timestamppb.New(time.UnixMicro(in.QueuedTimestampUsec)),
			WorkerStartTimestamp:           timestamppb.New(time.UnixMicro(in.WorkerStartTimestampUsec)),
			WorkerCompletedTimestamp:       timestamppb.New(time.UnixMicro(in.WorkerCompletedTimestampUsec)),
			InputFetchStartTimestamp:       timestamppb.New(time.UnixMicro(in.InputFetchStartTimestampUsec)),
			InputFetchCompletedTimestamp:   timestamppb.New(time.UnixMicro(in.InputFetchCompletedTimestampUsec)),
			ExecutionStartTimestamp:        timestamppb.New(time.UnixMicro(in.ExecutionStartTimestampUsec)),
			ExecutionCompletedTimestamp:    timestamppb.New(time.UnixMicro(in.ExecutionCompletedTimestampUsec)),
			OutputUploadStartTimestamp:     timestamppb.New(time.UnixMicro(in.OutputUploadStartTimestampUsec)),
			OutputUploadCompletedTimestamp: timestamppb.New(time.UnixMicro(in.OutputUploadCompletedTimestampUsec)),
			IoStats: &repb.IOStats{
				FileDownloadCount:        in.FileDownloadCount,
				FileDownloadSizeBytes:    in.FileDownloadSizeBytes,
				FileDownloadDurationUsec: in.FileDownloadDurationUsec,
				FileUploadCount:          in.FileUploadCount,
				FileUploadSizeBytes:      in.FileUploadSizeBytes,
				FileUploadDurationUsec:   in.FileUploadDurationUsec,
			},
			UsageStats: &repb.UsageStats{
				CpuNanos:        in.CPUNanos,
				PeakMemoryBytes: in.PeakMemoryBytes,
			},
		},
		CommandSnippet: in.CommandSnippet,
	}

	return out, nil
}

type ExecutionWithInvocationId struct {
	execution    *expb.Execution
	invocationID string
}

func (s *ExecutionSearchService) fetchExecutionData(ctx context.Context, groupId string, execIds []string) (map[string]*ExecutionWithInvocationId, error) {
	qString := "SELECT * FROM Executions WHERE (execution_id in ?) AND (perms & ? != 0) AND (group_id = ?)"
	qArgs := []interface{}{execIds, perms.GROUP_READ, groupId}

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
		exec, err := tableExecToProto(r)
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

// XXX: share
func validateAccessForStats(ctx context.Context, env environment.Env, groupID string) error {
	if err := perms.AuthorizeGroupAccess(ctx, env, groupID); err != nil {
		return err
	}
	if blocklist.IsBlockedForStatsQuery(groupID) {
		return status.ResourceExhaustedError("Too many rows.")
	}
	return nil
}

func (s *ExecutionSearchService) SearchExecutions(ctx context.Context, req *expb.SearchExecutionRequest) (*expb.SearchExecutionResponse, error) {
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return nil, err
	}
	if u.GetGroupID() == "" {
		return nil, status.InvalidArgumentError("An implicit group ID is required to search executions")
	}
	if err := validateAccessForStats(ctx, s.env, u.GetGroupID()); err != nil {
		return nil, err
	}

	q := query_builder.NewQuery(`SELECT * FROM Executions`)
	q.AddWhereClause("group_id = ?", u.GetGroupID())

	if user := req.GetQuery().GetUser(); user != "" {
		q.AddWhereClause("user = ?", user)
	}
	if host := req.GetQuery().GetHost(); host != "" {
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

	statusClauses := query_builder.OrClauses{}
	for _, status := range req.GetQuery().GetStatus() {
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

	// The underlying data is not precise enough to accurately support nanoseconds and there's no use case for it yet.
	if req.GetQuery().GetMinimumDuration().GetNanos() != 0 || req.GetQuery().GetMaximumDuration().GetNanos() != 0 {
		return nil, status.InvalidArgumentError("ExecutionSearchService does not support nanoseconds in duration queries")
	}

	if req.GetQuery().GetMinimumDuration().GetSeconds() != 0 {
		q.AddWhereClause(`duration_usec >= ?`, req.GetQuery().GetMinimumDuration().GetSeconds()*1000*1000)
	}
	if req.GetQuery().GetMaximumDuration().GetSeconds() != 0 {
		q.AddWhereClause(`duration_usec <= ?`, req.GetQuery().GetMaximumDuration().GetSeconds()*1000*1000)
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

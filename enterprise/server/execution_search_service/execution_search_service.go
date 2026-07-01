package execution_search_service

import (
	"context"
	"fmt"
	"log"
	"regexp"
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

var (
	runMatcher   = regexp.MustCompile(`_run_\d+_of_\d+`)
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

	q := query_builder.NewQuery(`
		SELECT worker_start_timestamp_usec, worker_completed_timestamp_usec, cpu_nanos, peak_memory_bytes, action_mnemonic, os, arch, output_path
		FROM "Executions"
	`)

	// Always filter to the currently selected (and authorized) group, and to
	// the requested target to constrain the scan size.
	q.AddWhereClause("group_id = ?", u.GetGroupID())
	q.AddWhereClause("target_label = ?", req.GetTarget())
	// Only include executions that actually ran on a worker; entries that never
	// started have nothing meaningful to plot on the timeline.
	q.AddWhereClause("worker_start_timestamp_usec > 0")
	q.AddWhereClause("worker_completed_timestamp_usec > 0")

	if err := s.addExecutionQueryFilters(q, req.GetQuery()); err != nil {
		return nil, err
	}

	q.SetOrderBy("worker_start_timestamp_usec", true)

	qString, qArgs := q.Build()
	olapExecutions, err := s.rawQueryExecutions(ctx, qString, qArgs...)
	if err != nil {
		return nil, err
	}
	groupedExecutions := make(map[string][]*schema.Execution)
	for _, ex := range olapExecutions {
		cleanedOutput := runMatcher.ReplaceAllString(ex.OutputPath, "")
		k := cleanedOutput + ex.ActionMnemonic + ex.OS + ex.Arch

		groupedExecutions[k] = append(groupedExecutions[k], ex)
	}

	rsp := &expb.GetExecutionTimelineResponse{
		Timelines: make([]*expb.ExecutionTimeline, 0, len(groupedExecutions)),
	}
	for _, tl := range groupedExecutions {
		executions := make([]*expb.ExecutionTimelineEntry, len(tl))
		for i, ex := range tl {
			executions[i] = &expb.ExecutionTimelineEntry{
				StartTimeUsec:   ex.WorkerStartTimestampUsec,
				DurationUsec:    ex.WorkerCompletedTimestampUsec - ex.WorkerStartTimestampUsec,
				CpuNanos:        ex.CPUNanos,
				PeakMemoryBytes: ex.PeakMemoryBytes,
			}
		}
		cleanedOutput := runMatcher.ReplaceAllString(tl[0].OutputPath, "")
		shardMatch := shardMatcher.FindStringSubmatch(cleanedOutput)
		log.Print(cleanedOutput)
		log.Printf("%+v", shardMatch)
		shard := int64(0)
		if len(shardMatch) > 1 {
			if realShard, err := strconv.Atoi(shardMatch[1]); err == nil {
				shard = int64(realShard)
			}
		}
		rsp.Timelines = append(rsp.Timelines, &expb.ExecutionTimeline{
			OutputPath: cleanedOutput,
			Mnemonic:   tl[0].ActionMnemonic,
			Os:         tl[0].OS,
			Arch:       tl[0].Arch,
			Shard:      shard,
			Execution:  executions,
		})
	}
	return rsp, nil
}

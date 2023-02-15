package invocation_search_service

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	sfpb "github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
)

const (
	// See defaultSortParams() for sort defaults.
	defaultLimitSize     = int64(15)
	pageSizeOffsetPrefix = "offset_"
)

type InvocationSearchService struct {
	env environment.Env
	h   interfaces.DBHandle
	oh  interfaces.OLAPDBHandle
}

func NewInvocationSearchService(env environment.Env, h interfaces.DBHandle, oh interfaces.OLAPDBHandle) *InvocationSearchService {
	return &InvocationSearchService{
		env: env,
		h:   h,
		oh:  oh,
	}
}

func defaultSortParams() *inpb.InvocationSort {
	return &inpb.InvocationSort{
		SortField: inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD,
		Ascending: false,
	}
}

func (s *InvocationSearchService) rawQueryInvocations(ctx context.Context, sql string, values ...interface{}) ([]*tables.Invocation, error) {
	rows, err := s.h.RawWithOptions(ctx, db.Opts().WithQueryName("search_invocations"), sql, values...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	invocations := make([]*tables.Invocation, 0)
	for rows.Next() {
		var ti tables.Invocation
		if err := s.h.DB(ctx).ScanRows(rows, &ti); err != nil {
			return nil, err
		}
		invocations = append(invocations, &ti)
	}
	return invocations, nil
}

func (s *InvocationSearchService) IndexInvocation(ctx context.Context, invocation *inpb.Invocation) error {
	// This is a no-op.
	return nil
}

func (s *InvocationSearchService) checkPreconditions(req *inpb.SearchInvocationRequest) error {
	if req.Query == nil {
		return status.InvalidArgumentError("The query field is required")
	}
	if req.Query.Host == "" && req.Query.User == "" && req.Query.CommitSha == "" && req.Query.RepoUrl == "" && req.Query.GroupId == "" {
		return status.InvalidArgumentError("At least one search atom must be set")
	}
	return nil
}

// TODO(tylerw): move this to a common place -- we'll use it a bunch.
func addPermissionsCheckToQuery(u interfaces.UserInfo, q *query_builder.Query) {
	o := query_builder.OrClauses{}
	o.AddOr("(i.perms & ? != 0)", perms.OTHERS_READ)
	groupArgs := []interface{}{
		perms.GROUP_READ,
	}
	groupParams := make([]string, 0)
	for _, g := range u.GetGroupMemberships() {
		groupArgs = append(groupArgs, g.GroupID)
		groupParams = append(groupParams, "?")
	}
	groupParamString := "(" + strings.Join(groupParams, ", ") + ")"
	groupQueryStr := fmt.Sprintf("(i.perms & ? != 0 AND i.group_id IN %s)", groupParamString)
	o.AddOr(groupQueryStr, groupArgs...)
	o.AddOr("(i.perms & ? != 0 AND i.user_id = ?)", perms.OWNER_READ, u.GetUserID())
	orQuery, orArgs := o.Build()
	q.AddWhereClause("("+orQuery+")", orArgs...)
}

func containsExecutionColumn(f []*sfpb.StatFilter) bool {
	for _, filter := range f {
		if filter.GetMetric().Execution != nil {
			return true
		}
	}
	return false
}

// This function adds where clauses that can be applied to both invocation- and
// execution-based searches.  Adding these filters to both phases of an
// execution-based search is duplicative, but we do it anyway to shrink the
// result set early while still ensure that our final search in the invocations
// table matches the query style of "normal" invocations.
func addCommonWhereClauses(q *query_builder.Query, iq *inpb.InvocationQuery) {
	// Don't include anonymous builds.
	q.AddWhereClause("((i.user_id != '' AND i.user_id IS NOT NULL) OR (i.group_id != '' AND i.group_id IS NOT NULL))")

	if user := iq.GetUser(); user != "" {
		q.AddWhereClause("i.user = ?", user)
	}
	if host := iq.GetHost(); host != "" {
		q.AddWhereClause("i.host = ?", host)
	}
	if url := iq.GetRepoUrl(); url != "" {
		q.AddWhereClause("i.repo_url = ?", url)
	}
	if branch := iq.GetBranchName(); branch != "" {
		q.AddWhereClause("i.branch_name = ?", branch)
	}
	if command := iq.GetCommand(); command != "" {
		q.AddWhereClause("i.command = ?", command)
	}
	if sha := iq.GetCommitSha(); sha != "" {
		q.AddWhereClause("i.commit_sha = ?", sha)
	}
	if group_id := iq.GetGroupId(); group_id != "" {
		q.AddWhereClause("i.group_id = ?", group_id)
	}
	roleClauses := query_builder.OrClauses{}
	for _, role := range iq.GetRole() {
		roleClauses.AddOr("i.role = ?", role)
	}
	if roleQuery, roleArgs := roleClauses.Build(); roleQuery != "" {
		q.AddWhereClause("("+roleQuery+")", roleArgs...)
	}

	statusClauses := query_builder.OrClauses{}
	for _, status := range iq.GetStatus() {
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
	statusQuery, statusArgs := statusClauses.Build()
	if statusQuery != "" {
		q.AddWhereClause(fmt.Sprintf("(%s)", statusQuery), statusArgs...)
	}
}

func (s *InvocationSearchService) searchInvocationsInExecutions(ctx context.Context, req *inpb.SearchInvocationRequest) (*query_builder.Query, error) {
	if s.oh == nil {
		return nil, status.UnimplementedError("Execution filters require an OLAP DB.")
	}
	q := query_builder.NewQuery(`SELECT DISTINCT invocation_uuid FROM Executions as i`)
	addCommonWhereClauses(q, req.GetQuery())
	for _, f := range req.GetQuery().GetFilter() {
		if f.GetMetric().Execution == nil {
			continue
		}
		str, args, err := filter.GenerateFilterStringAndArgs(f, "i.")
		if err != nil {
			return nil, err
		}
		q.AddWhereClause(str, args...)
	}

	// Set a high-ish limit here, but not the request's limit: there may be extra
	// filters when we go to fetch the actual invocations that filter out even
	// more results.
	q.SetLimit(200)
	qString, qArgs := q.Build()

	if req.Sort != nil || req.PageToken != "" {
		return nil, status.UnimplementedError("Sorting and pagination is not supported for execution searches.")
	}
	q.SetOrderBy("i.updated_at_usec", false)

	// More stuff...
	rows, err := s.oh.DB(ctx).Raw(qString, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	type o struct {
		InvocationUuid string
	}
	var ids []string
	for rows.Next() {
		out := o{}
		if err := s.oh.DB(ctx).ScanRows(rows, &out); err != nil {
			return nil, err
		}
		u, err := uuid.Parse(out.InvocationUuid)
		if err != nil {
			return nil, err
		}

		ids = append(ids, u.String())
	}

	invocationQuery := query_builder.NewQuery("SELECT * FROM Invocations AS i")
	invocationQuery.AddWhereClause("i.invocation_id IN ?", ids)
	return invocationQuery, nil
}

func (s *InvocationSearchService) QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	if err := s.checkPreconditions(req); err != nil {
		return nil, err
	}
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return nil, err
	}
	if blocklist.IsBlockedForStatsQuery(u.GetGroupID()) {
		return nil, status.ResourceExhaustedErrorf("Too many rows.")
	}

	isExecution := containsExecutionColumn(req.GetQuery().GetFilter())
	var q *query_builder.Query
	if isExecution {
		q, err = s.searchInvocationsInExecutions(ctx, req)
		if err != nil {
			return nil, err
		}
	} else {
		q = query_builder.NewQuery(`SELECT * FROM Invocations AS i`)
	}

	addCommonWhereClauses(q, req.GetQuery())
	if start := req.GetQuery().GetUpdatedAfter(); start.IsValid() {
		q.AddWhereClause("i.updated_at_usec >= ?", start.AsTime().UnixMicro())
	}
	if end := req.GetQuery().GetUpdatedBefore(); end.IsValid() {
		q.AddWhereClause("i.updated_at_usec < ?", end.AsTime().UnixMicro())
	}

	// The underlying data is not precise enough to accurately support nanoseconds and there's no use case for it yet.
	if req.GetQuery().GetMinimumDuration().GetNanos() != 0 || req.GetQuery().GetMaximumDuration().GetNanos() != 0 {
		return nil, status.InvalidArgumentError("InvocationSearchService does not support nanoseconds in duration queries")
	}

	if req.GetQuery().GetMinimumDuration().GetSeconds() != 0 {
		q.AddWhereClause(`duration_usec >= ?`, req.GetQuery().GetMinimumDuration().GetSeconds()*1000*1000)
	}
	if req.GetQuery().GetMaximumDuration().GetSeconds() != 0 {
		q.AddWhereClause(`duration_usec <= ?`, req.GetQuery().GetMaximumDuration().GetSeconds()*1000*1000)
	}

	for _, f := range req.GetQuery().GetFilter() {
		if f.GetMetric().Invocation == nil {
			continue
		}
		str, args, err := filter.GenerateFilterStringAndArgs(f, "i.")
		if err != nil {
			return nil, err
		}
		q.AddWhereClause(str, args...)
	}

	// Always add permissions check.
	addPermissionsCheckToQuery(u, q)

	sort := req.Sort
	if sort == nil {
		sort = defaultSortParams()
	} else if req.Sort.SortField == inpb.InvocationSort_UNKNOWN_SORT_FIELD {
		sort.SortField = defaultSortParams().SortField
	}
	switch sort.SortField {
	case inpb.InvocationSort_CREATED_AT_USEC_SORT_FIELD:
		q.SetOrderBy("created_at_usec", sort.Ascending)
	case inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD:
		q.SetOrderBy("updated_at_usec", sort.Ascending)
	case inpb.InvocationSort_DURATION_SORT_FIELD:
		q.SetOrderBy("duration_usec", sort.Ascending)
	case inpb.InvocationSort_ACTION_CACHE_HIT_RATIO_SORT_FIELD:
		// Treat 0/0 as 100% cache hit rate to avoid divide-by-zero weirdness.
		q.SetOrderBy(`IFNULL(
			action_cache_hits / (action_cache_hits + action_cache_misses), 1)`,
			sort.Ascending)
	case inpb.InvocationSort_CONTENT_ADDRESSABLE_STORE_CACHE_HIT_RATIO_SORT_FIELD:
		// Treat 0/0 as 100% cache hit rate to avoid divide-by-zero weirdness.
		q.SetOrderBy(`IFNULL(
			cas_cache_hits / (cas_cache_hits + cas_cache_misses), 1)`,
			sort.Ascending)
	case inpb.InvocationSort_CACHE_DOWNLOADED_SORT_FIELD:
		q.SetOrderBy("total_download_size_bytes", sort.Ascending)
	case inpb.InvocationSort_CACHE_UPLOADED_SORT_FIELD:
		q.SetOrderBy("total_upload_size_bytes", sort.Ascending)
	case inpb.InvocationSort_CACHE_TRANSFERRED_SORT_FIELD:
		q.SetOrderBy("total_download_size_bytes + total_upload_size_bytes",
			sort.Ascending)
	case inpb.InvocationSort_UNKNOWN_SORT_FIELD:
		alert.UnexpectedEvent("invocation_search_no_sort_order")
	}

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
	tableInvocations, err := s.rawQueryInvocations(ctx, qString, qArgs...)
	if err != nil {
		return nil, err
	}
	rsp := &inpb.SearchInvocationResponse{}
	for _, ti := range tableInvocations {
		rsp.Invocation = append(rsp.Invocation, build_event_handler.TableInvocationToProto(ti))
	}
	if int64(len(rsp.Invocation)) == limitSize {
		rsp.NextPageToken = pageSizeOffsetPrefix + strconv.FormatInt(offset+limitSize, 10)
	}
	return rsp, nil
}

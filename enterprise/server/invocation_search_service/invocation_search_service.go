package invocation_search_service

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

const (
	// See defaultSortParams() for sort defaults.
	defaultLimitSize     = int64(15)
	pageSizeOffsetPrefix = "offset_"
)

var (
	olapInvocationSearchEnabled = flag.Bool("app.olap_invocation_search_enabled", true, "If true, InvocationSearchService will query clickhouse for some queries.")
)

type InvocationSearchService struct {
	env     environment.Env
	dbh     interfaces.DBHandle
	olapdbh interfaces.OLAPDBHandle
}

func NewInvocationSearchService(env environment.Env, h interfaces.DBHandle, oh interfaces.OLAPDBHandle) *InvocationSearchService {
	return &InvocationSearchService{
		env:     env,
		dbh:     h,
		olapdbh: oh,
	}
}

func defaultSortParams() *inpb.InvocationSort {
	return &inpb.InvocationSort{
		SortField: inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD,
		Ascending: false,
	}
}

// Clickhouse UUIDs are formatted without any dashes :(
func fixUUID(uuid string) (string, error) {
	if len(uuid) != 32 {
		return "", status.InternalErrorf("Tried to fetch an invalid invocation ID %s", uuid)
	}
	return uuid[0:8] + "-" + uuid[8:12] + "-" + uuid[12:16] + "-" + uuid[16:20] + "-" + uuid[20:32], nil
}

func (s *InvocationSearchService) hydrateInvocationsFromDB(ctx context.Context, invocationIds []string, sort *inpb.InvocationSort) ([]*inpb.Invocation, error) {
	q := query_builder.NewQuery(`SELECT * FROM "Invocations" as i`)
	q.AddWhereClause("i.invocation_id IN ?", invocationIds)
	addOrderBy(sort, q)
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return nil, err
	}
	addPermissionsCheckToQuery(u, q)

	qStr, qArgs := q.Build()

	rows, err := s.dbh.RawWithOptions(ctx, db.Opts().WithQueryName("hydrate_invocation_search"), qStr, qArgs...).Rows()
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	out := make([]*tables.Invocation, 0)
	for rows.Next() {
		var ti tables.Invocation
		if err := s.dbh.DB(ctx).ScanRows(rows, &ti); err != nil {
			return nil, err
		}
		out = append(out, &ti)
	}

	invocations := make([]*inpb.Invocation, 0)
	for _, ti := range out {
		invocations = append(invocations, build_event_handler.TableInvocationToProto(ti))
	}

	return invocations, nil
}

func (s *InvocationSearchService) rawQueryInvocationsFromClickhouse(ctx context.Context, req *inpb.SearchInvocationRequest, offset int64, limit int64) ([]*inpb.Invocation, int64, error) {
	sql, args, err := s.buildPrimaryQuery(ctx, "invocation_uuid", offset, limit, req)
	if err != nil {
		return nil, 0, err
	}
	rows, err := s.olapdbh.RawWithOptions(ctx, clickhouse.Opts().WithQueryName("clickhouse_search_invocations"), sql, args...).Rows()
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	tis := make([]string, 0)
	for rows.Next() {
		var ti schema.Invocation
		if err := s.olapdbh.DB(ctx).ScanRows(rows, &ti); err != nil {
			return nil, 0, err
		}
		fixedUUID, err := fixUUID(ti.InvocationUUID)
		if err != nil {
			return nil, 0, err
		}
		tis = append(tis, fixedUUID)
	}

	invocations, err := s.hydrateInvocationsFromDB(ctx, tis, req.GetSort())
	// It's possible but unlikely that some of the invocations we find in
	// Clickhouse can't be found in the main database.  In this case, we
	// silently drop these invocations but still use the number of
	// invocations returned by Clickhouse as the offset for future queries
	// so that the pagination offset picks up from the right place.
	return invocations, int64(len(tis)), err
}

func (s *InvocationSearchService) rawQueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest, offset int64, limit int64) ([]*inpb.Invocation, int64, error) {
	sql, args, err := s.buildPrimaryQuery(ctx, "*", offset, limit, req)
	if err != nil {
		return nil, 0, err
	}
	rows, err := s.dbh.RawWithOptions(ctx, db.Opts().WithQueryName("search_invocations"), sql, args...).Rows()
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	tis := make([]*tables.Invocation, 0)
	for rows.Next() {
		var ti tables.Invocation
		if err := s.dbh.DB(ctx).ScanRows(rows, &ti); err != nil {
			return nil, 0, err
		}
		tis = append(tis, &ti)
	}

	invocations := make([]*inpb.Invocation, 0)
	for _, ti := range tis {
		invocations = append(invocations, build_event_handler.TableInvocationToProto(ti))
	}

	return invocations, int64(len(invocations)), nil
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
	q = q.AddWhereClause("("+orQuery+")", orArgs...)
}

func (s *InvocationSearchService) shouldQueryClickhouse(req *inpb.SearchInvocationRequest) bool {
	return s.olapdbh != nil && *olapInvocationSearchEnabled && len(req.GetQuery().GetFilter()) > 0
}

func addOrderBy(sort *inpb.InvocationSort, q *query_builder.Query) {
	if sort == nil {
		sort = defaultSortParams()
	} else if sort.SortField == inpb.InvocationSort_UNKNOWN_SORT_FIELD {
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
}

func (s *InvocationSearchService) buildPrimaryQuery(ctx context.Context, fields string, offset int64, limit int64, req *inpb.SearchInvocationRequest) (string, []interface{}, error) {
	if req.GetQuery().GetRepoUrl() != "" {
		norm, err := git.NormalizeRepoURL(req.GetQuery().GetRepoUrl())
		if err == nil { // if we normalized successfully
			req.Query.RepoUrl = norm.String()
		}
	}

	if err := s.checkPreconditions(req); err != nil {
		return "", nil, err
	}
	u, err := perms.AuthenticatedUser(ctx, s.env)
	if err != nil {
		return "", nil, err
	}
	if blocklist.IsBlockedForStatsQuery(u.GetGroupID()) {
		return "", nil, status.ResourceExhaustedErrorf("Too many rows.")
	}
	q := query_builder.NewQuery(fmt.Sprintf(`SELECT %s FROM "Invocations" as i`, fields))

	// Don't include anonymous builds.
	q.AddWhereClause("((i.user_id != '' AND i.user_id IS NOT NULL) OR (i.group_id != '' AND i.group_id IS NOT NULL))")

	if user := req.GetQuery().GetUser(); user != "" {
		q.AddWhereClause("i.user = ?", user)
	}
	if host := req.GetQuery().GetHost(); host != "" {
		q.AddWhereClause("i.host = ?", host)
	}
	if url := req.GetQuery().GetRepoUrl(); url != "" {
		q.AddWhereClause("i.repo_url = ?", url)
	}
	if branch := req.GetQuery().GetBranchName(); branch != "" {
		q.AddWhereClause("i.branch_name = ?", branch)
	}
	if command := req.GetQuery().GetCommand(); command != "" {
		q.AddWhereClause("i.command = ?", command)
	}
	if pattern := req.GetQuery().GetPattern(); pattern != "" {
		q.AddWhereClause("i.pattern = ?", pattern)
	}
	if sha := req.GetQuery().GetCommitSha(); sha != "" {
		q.AddWhereClause("i.commit_sha = ?", sha)
	}
	if group_id := req.GetQuery().GetGroupId(); group_id != "" {
		q.AddWhereClause("i.group_id = ?", group_id)
	}
	roleClauses := query_builder.OrClauses{}
	for _, role := range req.GetQuery().GetRole() {
		roleClauses.AddOr("i.role = ?", role)
	}
	if roleQuery, roleArgs := roleClauses.Build(); roleQuery != "" {
		q.AddWhereClause("("+roleQuery+")", roleArgs...)
	}
	if start := req.GetQuery().GetUpdatedAfter(); start.IsValid() {
		q.AddWhereClause("i.updated_at_usec >= ?", start.AsTime().UnixMicro())
	}
	if end := req.GetQuery().GetUpdatedBefore(); end.IsValid() {
		q.AddWhereClause("i.updated_at_usec < ?", end.AsTime().UnixMicro())
	}

	statusClauses := query_builder.OrClauses{}
	for _, status := range req.GetQuery().GetStatus() {
		switch status {
		case inspb.OverallStatus_SUCCESS:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS), 1)
		case inspb.OverallStatus_FAILURE:
			statusClauses.AddOr(`(invocation_status = ? AND success = ?)`, int(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS), 0)
		case inspb.OverallStatus_IN_PROGRESS:
			statusClauses.AddOr(`invocation_status = ?`, int(inspb.InvocationStatus_PARTIAL_INVOCATION_STATUS))
		case inspb.OverallStatus_DISCONNECTED:
			statusClauses.AddOr(`invocation_status = ?`, int(inspb.InvocationStatus_DISCONNECTED_INVOCATION_STATUS))
		case inspb.OverallStatus_UNKNOWN_OVERALL_STATUS:
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
		return "", nil, status.InvalidArgumentError("InvocationSearchService does not support nanoseconds in duration queries")
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
			return "", nil, err
		}
		q.AddWhereClause(str, args...)
	}

	// Clickhouse doesn't hold permissions data, but we need to *always*
	// check permissions when we query from the main DB.  This is handled
	// here for the non-Clickhouse case, and at the hydration step when
	// querying clickhouse.
	if !s.shouldQueryClickhouse(req) {
		addPermissionsCheckToQuery(u, q)
	}

	addOrderBy(req.Sort, q)
	q.SetLimit(limit)

	q.SetOffset(offset)
	qStr, qArgs := q.Build()
	return qStr, qArgs, nil
}

func computeOffsetAndLimit(req *inpb.SearchInvocationRequest) (int64, int64, error) {
	offset := int64(0)
	if strings.HasPrefix(req.PageToken, pageSizeOffsetPrefix) {
		parsedOffset, err := strconv.ParseInt(strings.Replace(req.PageToken, pageSizeOffsetPrefix, "", 1), 10, 64)
		if err != nil {
			return 0, 0, status.InvalidArgumentError("Error parsing pagination token")
		}
		offset = parsedOffset
	} else if req.PageToken != "" {
		return 0, 0, status.InvalidArgumentError("Invalid pagination token")
	}

	limit := defaultLimitSize
	if req.Count > 0 {
		limit = int64(req.Count)
	}

	return offset, limit, nil

}

func (s *InvocationSearchService) QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	offset, limit, err := computeOffsetAndLimit(req)
	if err != nil {
		return nil, err
	}

	var invocations []*inpb.Invocation
	var count int64
	if s.shouldQueryClickhouse(req) {
		invocations, count, err = s.rawQueryInvocationsFromClickhouse(ctx, req, offset, limit)
	} else {
		invocations, count, err = s.rawQueryInvocations(ctx, req, offset, limit)
	}
	if err != nil {
		return nil, err
	}

	rsp := &inpb.SearchInvocationResponse{Invocation: invocations}
	if count == limit {
		rsp.NextPageToken = pageSizeOffsetPrefix + strconv.FormatInt(offset+limit, 10)
	}
	return rsp, nil
}

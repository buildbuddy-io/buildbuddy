package invocation_search_service

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	sfpb "github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
)

const (
	// See defaultSortParams() for sort defaults.
	defaultLimitSize     = int64(15)
	pageSizeOffsetPrefix = "offset_"
)

var (
	blendedInvocationSearchEnabled = flag.Bool("app.blended_invocation_search_enabled", false, "If true, InvocationSearchService will query clickhouse for all searches, filling in in-progress invocations from the regular DB.")
	olapInvocationSearchEnabled    = flag.Bool("app.olap_invocation_search_enabled", true, "If true, InvocationSearchService will query clickhouse for a few impossibly slow queries (i.e., tags), but mostly use the regular DB.")
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

func (s *InvocationSearchService) hydrateInvocationsFromDB(ctx context.Context, invocationIds []string, sort *inpb.InvocationSort) ([]*inpb.Invocation, error) {
	q := query_builder.NewQuery(`SELECT * FROM "Invocations" as i`)
	q.AddWhereClause("i.invocation_id IN ?", invocationIds)
	addOrderBy(sort, q)
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	addPermissionsCheckToQuery(u, q)

	qStr, qArgs := q.Build()

	rq := s.dbh.NewQuery(ctx, "invocation_search_hydrate").Raw(qStr, qArgs...)
	out, err := db.ScanAll(rq, &tables.Invocation{})
	if err != nil {
		return nil, err
	}

	invocations := make([]*inpb.Invocation, 0)
	for _, ti := range out {
		invocations = append(invocations, build_event_handler.TableInvocationToProto(ti))
	}

	return invocations, nil
}

func (s *InvocationSearchService) rawQueryInvocationsFromClickhouse(ctx context.Context, req *inpb.SearchInvocationRequest, offset int64, limit int64) ([]*inpb.Invocation, int64, error) {
	sql, args, err := s.buildPrimaryQuery(ctx, "invocation_uuid", offset, limit, req, true)
	if err != nil {
		return nil, 0, err
	}
	rq := s.olapdbh.NewQuery(ctx, "invocation_search_service_search").Raw(sql, args...)
	tis := make([]string, 0)
	err = db.ScanEach(rq, func(ctx context.Context, ti *schema.Invocation) error {
		fixedUUID, err := uuid.Base64StringToString(ti.InvocationUUID)
		if err != nil {
			return err
		}
		tis = append(tis, fixedUUID)
		return nil
	})
	if err != nil {
		return nil, 0, err
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
	sql, args, err := s.buildPrimaryQuery(ctx, "*", offset, limit, req, false)
	if err != nil {
		return nil, 0, err
	}
	rq := s.dbh.NewQuery(ctx, "invocation_search").Raw(sql, args...)
	tis, err := db.ScanAll(rq, &tables.Invocation{})
	if err != nil {
		return nil, 0, err
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
	q.AddWhereClause("("+orQuery+")", orArgs...)
}

func (s *InvocationSearchService) shouldQueryClickhouse(req *inpb.SearchInvocationRequest) bool {
	olapSearchEnabled := *olapInvocationSearchEnabled && (len(req.GetQuery().GetTags()) > 0 || len(req.GetQuery().GetFilter()) > 0)
	return s.olapdbh != nil && (olapSearchEnabled || shouldUseBlendedSearch(req))
}

func shouldUseBlendedSearch(req *inpb.SearchInvocationRequest) bool {
	sort := req.Sort
	if sort == nil {
		sort = defaultSortParams()
	} else if sort.SortField == inpb.InvocationSort_UNKNOWN_SORT_FIELD {
		sort.SortField = defaultSortParams().SortField
	}
	isSupportedSort := sort.SortField == inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD

	return *blendedInvocationSearchEnabled && isSupportedSort && requestIncludesUnfinishedBuilds(req)
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

func (s *InvocationSearchService) buildPrimaryQuery(ctx context.Context, fields string, offset int64, limit int64, req *inpb.SearchInvocationRequest, isOlapQuery bool) (string, []interface{}, error) {
	if req.GetQuery().GetRepoUrl() != "" {
		norm, err := git.NormalizeRepoURL(req.GetQuery().GetRepoUrl())
		if err == nil { // if we normalized successfully
			req.Query.RepoUrl = norm.String()
		}
	}

	if err := s.checkPreconditions(req); err != nil {
		return "", nil, err
	}
	u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
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
	if tags := req.GetQuery().GetTags(); len(tags) > 0 {
		if isOlapQuery {
			clause, args := invocation_format.GetTagsAsClickhouseWhereClause("i.tags", tags)
			q.AddWhereClause(clause, args...)
		} else if s.dbh.GORM(ctx, "dialector").Dialector.Name() == "mysql" {
			for _, tag := range tags {
				q.AddWhereClause("FIND_IN_SET(?, i.tags)", tag)
			}
		} else {
			for _, tag := range tags {
				q.AddWhereClause("i.tags LIKE ?", "%"+strings.ReplaceAll(tag, "%", "\\%")+"%")
			}
		}
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
	if !isOlapQuery {
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

func requestIncludesUnfinishedBuilds(req *inpb.SearchInvocationRequest) bool {
	if len(req.GetQuery().GetStatus()) == 0 {
		// No filters on status, so the query includes both pending and disconnected builds.
		return true
	}
	for _, s := range req.GetQuery().GetStatus() {
		if s == inspb.OverallStatus_IN_PROGRESS || s == inspb.OverallStatus_DISCONNECTED {
			return true
		}
	}
	return false
}

func compareInvocationsBySortField(a *inpb.Invocation, b *inpb.Invocation, sort *inpb.InvocationSort) bool {
	var v1, v2 int64
	switch sort.SortField {
	case inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD:
		v1 = a.GetUpdatedAtUsec()
		v2 = b.GetUpdatedAtUsec()
	default:
		alert.UnexpectedEvent("blended_invocation_search_unsupported_sort")
	}
	if sort.Ascending {
		return v1 < v2
	}
	return v1 > v2
}

func getFilterValue(inv *inpb.Invocation, sortField inpb.InvocationSort_SortField) int64 {
	switch sortField {
	case inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD:
		return inv.GetUpdatedAtUsec()
	default:
		alert.UnexpectedEvent("blended_invocation_search_unsupported_sort")
	}
	return -1
}

func getExtraFilterForBlendedQuery(firstResult *inpb.Invocation, lastResult *inpb.Invocation, sort *inpb.InvocationSort) *sfpb.StatFilter {
	if firstResult == nil && lastResult == nil {
		return nil
	}
	f := &sfpb.StatFilter{}
	switch sort.SortField {
	case inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD:
		metric := sfpb.InvocationMetricType_UPDATED_AT_USEC_INVOCATION_METRIC
		f.Metric = &sfpb.Metric{Invocation: &metric}
	default:
		alert.UnexpectedEvent("blended_invocation_search_unsupported_sort")
	}
	if firstResult != nil {
		v := getFilterValue(firstResult, sort.SortField)
		if sort.Ascending {
			f.Min = &v
		} else {
			f.Max = &v
		}
	}
	if lastResult != nil {
		v := getFilterValue(lastResult, sort.SortField)
		if sort.Ascending {
			f.Max = &v
		} else {
			f.Min = &v
		}
	}
	return f
}

func mergeSortedInvocations(a []*inpb.Invocation, b []*inpb.Invocation, sort *inpb.InvocationSort) []*inpb.Invocation {
	if len(a) == 0 {
		return b
	}
	if sort == nil {
		sort = defaultSortParams()
	}
	i := 0
	j := 0
	out := make([]*inpb.Invocation, 0, len(a)+len(b))
	for i <= len(a) && j <= len(b) {
		if i == len(a) {
			out = append(out, b[j:]...)
			break
		}
		if j == len(b) {
			out = append(out, a[i:]...)
			break
		}
		if compareInvocationsBySortField(a[i], b[j], sort) {
			out = append(out, a[i])
			i++
		} else {
			out = append(out, b[j])
			j++
		}
	}
	return out
}

func (s *InvocationSearchService) mergeUnfinishedBuildsFromMysql(ctx context.Context, olapInvocations []*inpb.Invocation, isLastPage bool, req *inpb.SearchInvocationRequest, offset int64, limit int64) ([]*inpb.Invocation, error) {
	sqlReq := proto.Clone(req).(*inpb.SearchInvocationRequest)

	// Add new filters to only include range we saw from the OLAP DB.
	// Clamping down to the relevant range based on sort order means
	// that we will scan way less data in mysql looking for pending
	// and disconnected builds regardless of other filters on the
	// query.
	if len(olapInvocations) > 0 {
		var firstResult, lastResult *inpb.Invocation
		// If this is the first page of results, don't filter left side.
		if offset == 0 {
			firstResult = nil
		} else {
			firstResult = olapInvocations[0]
		}
		// If this is the last page of results, don't filter the right side.
		if isLastPage {
			lastResult = nil
		} else {
			lastResult = olapInvocations[len(olapInvocations)-1]
		}

		if f := getExtraFilterForBlendedQuery(firstResult, lastResult, req.GetSort()); f != nil {
			sqlReq.GetQuery().Filter = append(sqlReq.GetQuery().Filter, f)
		}
	}

	// Add filters for pending + disconnected only.
	newStatusFilters := make([]inspb.OverallStatus, 0)
	if len(sqlReq.Query.Status) == 0 {
		newStatusFilters = append(newStatusFilters, inspb.OverallStatus_IN_PROGRESS, inspb.OverallStatus_DISCONNECTED)
	} else {
		for _, s := range sqlReq.Query.Status {
			if s == inspb.OverallStatus_IN_PROGRESS || s == inspb.OverallStatus_DISCONNECTED {
				newStatusFilters = append(newStatusFilters, s)
			}
		}
	}
	sqlReq.GetQuery().Status = newStatusFilters

	// No offset here as we're depending on output from
	// getExtraFilterForBlendedQuery to offset the returned results.
	sqlInvocations, _, err := s.rawQueryInvocations(ctx, sqlReq, 0, limit)
	if err != nil {
		return nil, err
	}
	if len(sqlInvocations) == int(limit) {
		log.Warningf("An invocation query fetched more than one page of results from mysql: %v", sqlReq)
	}

	return mergeSortedInvocations(olapInvocations, sqlInvocations, req.Sort), nil
}

func (s *InvocationSearchService) QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	offset, limit, err := computeOffsetAndLimit(req)
	if err != nil {
		return nil, err
	}
	if req.Sort == nil {
		req.Sort = defaultSortParams()
	} else if req.Sort.SortField == inpb.InvocationSort_UNKNOWN_SORT_FIELD {
		req.Sort.SortField = defaultSortParams().SortField
	}

	var invocations []*inpb.Invocation
	var count int64
	if s.shouldQueryClickhouse(req) {
		invocations, count, err = s.rawQueryInvocationsFromClickhouse(ctx, req, offset, limit)
		if err != nil {
			return nil, err
		}
		if shouldUseBlendedSearch(req) {
			invocations, err = s.mergeUnfinishedBuildsFromMysql(ctx, invocations, limit != count /* isLastPage */, req, offset, limit)
			if err != nil {
				return nil, err
			}
		}
	} else {
		invocations, count, err = s.rawQueryInvocations(ctx, req, offset, limit)
	}
	if err != nil {
		return nil, err
	}

	rsp := &inpb.SearchInvocationResponse{Invocation: invocations}
	if count == limit {
		nextPageStart := offset + limit
		if shouldUseBlendedSearch(req) {
			// Woohoo, a hack! We fetch invocations from mysql based on the
			// range of values we see in clickhouse.  We can either send
			// information about how many results were fetched from mySQL to the
			// client in NextPageToken or play this goofy trick, where we drop
			// the last result from clickhouse (and expect to fetch it on the
			// next request).  This way is less crufty as an interface, but it
			// means we don't support cases where the mySQL requests returns
			// more than the specified limit.
			nextPageStart = nextPageStart - 1
			rsp.Invocation = rsp.Invocation[:(len(rsp.Invocation) - 1)]
		}
		rsp.NextPageToken = pageSizeOffsetPrefix + strconv.FormatInt(nextPageStart, 10)
	}
	return rsp, nil
}

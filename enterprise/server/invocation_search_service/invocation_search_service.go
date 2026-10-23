package invocation_search_service

import (
	"context"
	"flag"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/backends/invocationdb"
	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/filter"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"google.golang.org/protobuf/reflect/protoreflect"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	sfpb "github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
	gproto "google.golang.org/protobuf/proto"
)

const (
	// See defaultSortParams() for sort defaults.
	defaultLimitSize     = int64(15)
	pageSizeOffsetPrefix = "offset_"
)

var (
	olapInvocationSearchEnabled = flag.Bool("app.olap_invocation_search_enabled", true, "If true, InvocationSearchService will query clickhouse for a few impossibly slow queries (i.e., tags), but mostly use the regular DB.")
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
	q := query_builder.NewQuery(`SELECT * FROM "Invocations"`)
	q.AddWhereClause("invocation_id IN ?", invocationIds)
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
		invocations = append(invocations, invocationdb.TableInvocationToProto(ti))
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
		invocations = append(invocations, invocationdb.TableInvocationToProto(ti))
	}

	return invocations, int64(len(invocations)), nil
}

func (s *InvocationSearchService) IndexInvocation(ctx context.Context, invocation *inpb.Invocation) error {
	// This is a no-op.
	return nil
}

// TODO(tylerw): move this to a common place -- we'll use it a bunch.
func addPermissionsCheckToQuery(u interfaces.UserInfo, q *query_builder.Query) {
	o := query_builder.OrClauses{}
	o.AddOr("(perms & ? != 0)", perms.OTHERS_READ)
	groupArgs := []any{
		perms.GROUP_READ,
	}
	groupParams := make([]string, 0)
	for _, g := range u.GetGroupMemberships() {
		groupArgs = append(groupArgs, g.GroupID)
		groupParams = append(groupParams, "?")
	}
	groupParamString := "(" + strings.Join(groupParams, ", ") + ")"
	groupQueryStr := fmt.Sprintf("(perms & ? != 0 AND group_id IN %s)", groupParamString)
	o.AddOr(groupQueryStr, groupArgs...)
	o.AddOr("(perms & ? != 0 AND user_id = ?)", perms.OWNER_READ, u.GetUserID())
	orQuery, orArgs := o.Build()
	q.AddWhereClause("("+orQuery+")", orArgs...)
}

func containsArrayFilters(filters []*sfpb.GenericFilter) bool {
	for _, f := range filters {
		typeDescriptorOptions := protoreflect.EnumValueDescriptor(f.GetType().Descriptor().Values().ByNumber(f.GetType().Number())).Options()
		if typeDescriptorOptions == nil {
			continue
		}
		typeOptions := gproto.GetExtension(typeDescriptorOptions, sfpb.E_FilterTypeOptions).(*sfpb.FilterTypeOptions)
		if typeOptions == nil {
			continue
		}
		if typeOptions.GetCategory() == sfpb.FilterCategory_STRING_ARRAY_FILTER_CATEGORY {
			return true
		}
	}
	return false
}

func (s *InvocationSearchService) shouldQueryClickhouse(req *inpb.SearchInvocationRequest) bool {
	olapSearchEnabled := *olapInvocationSearchEnabled && (len(req.GetQuery().GetTags()) > 0 || len(req.GetQuery().GetFilter()) > 0 || containsArrayFilters(req.GetQuery().GetGenericFilters()))
	return s.olapdbh != nil && olapSearchEnabled
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

func (s *InvocationSearchService) buildPrimaryQuery(ctx context.Context, fields string, offset int64, limit int64, req *inpb.SearchInvocationRequest, isOlapQuery bool) (string, []any, error) {
	if req.GetQuery().GetRepoUrl() != "" {
		norm, err := git.NormalizeRepoURL(req.GetQuery().GetRepoUrl())
		if err == nil { // if we normalized successfully
			req.Query.RepoUrl = norm.String()
		}
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
	q.AddWhereClause("((user_id != '' AND user_id IS NOT NULL) OR (group_id != '' AND group_id IS NOT NULL))")

	if user := req.GetQuery().GetUser(); user != "" {
		q.AddWhereClause("\"user\" = ?", user)
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
	if group_id := req.GetQuery().GetGroupId(); group_id != "" {
		q.AddWhereClause("group_id = ?", group_id)
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
		if isOlapQuery {
			clause, args := invocation_format.GetTagsAsClickhouseWhereClause("tags", tags)
			q.AddWhereClause(clause, args...)
		} else if s.dbh.GORM(ctx, "dialector").Dialector.Name() == "mysql" {
			for _, tag := range tags {
				q.AddWhereClause("FIND_IN_SET(?, tags)", tag)
			}
		} else {
			for _, tag := range tags {
				q.AddWhereClause("tags LIKE ?", "%"+strings.ReplaceAll(tag, "%", "\\%")+"%")
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

	if minDuration := req.GetQuery().GetMinimumDuration().AsDuration(); minDuration > 0 {
		q.AddWhereClause(`duration_usec >= ?`, minDuration.Microseconds())
	}
	if maxDuration := req.GetQuery().GetMaximumDuration().AsDuration(); maxDuration > 0 {
		q.AddWhereClause(`duration_usec <= ?`, maxDuration.Microseconds())
	}

	for _, f := range req.GetQuery().GetFilter() {
		if f.GetMetric().Invocation == nil {
			continue
		}
		str, args, err := filter.GenerateFilterStringAndArgs(f)
		if err != nil {
			return "", nil, err
		}
		q.AddWhereClause(str, args...)
	}
	for _, f := range req.GetQuery().GetDimensionFilter() {
		if f.GetDimension().Invocation == nil {
			continue
		}
		str, args, err := filter.GenerateDimensionFilterStringAndArgs(f)
		if err != nil {
			return "", nil, err
		}
		q.AddWhereClause(str, args...)
	}

	dialectName := s.dbh.DialectName()
	if isOlapQuery {
		dialectName = s.olapdbh.DialectName()
	}

	err = filter.AddGenericFiltersToQuery(q, req.GetQuery().GetGenericFilters(), sfpb.ObjectTypes_INVOCATION_OBJECTS, dialectName)
	if err != nil {
		return "", nil, err
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

	// Default the query group_id to the authenticated group ID.
	if req.GetQuery().GetGroupId() == "" {
		req = req.CloneVT()
		if req.Query == nil {
			req.Query = &inpb.InvocationQuery{}
		}
		u, err := s.env.GetAuthenticator().AuthenticatedUser(ctx)
		// Note: the two cases below should not happen in practice because
		// capabilities_filter requires that the user is authenticated and that
		// they are a member of a group.
		if err != nil {
			return nil, status.InternalErrorf("user is unexpectedly unauthenticated: %v", err)
		}
		if u.GetGroupID() == "" {
			return nil, status.InternalError("user is unexpectedly missing group ID")
		}
		req.Query.GroupId = u.GetGroupID()
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

func (s *InvocationSearchService) GetInvocationFilterSuggestions(ctx context.Context, req *inpb.GetInvocationFilterSuggestionsRequest) (*inpb.GetInvocationFilterSuggestionsResponse, error) {
	if err := authutil.AuthorizeGroupAccessForStats(ctx, s.env, req.GetRequestContext().GetGroupId()); err != nil {
		return nil, err
	}

	if s.olapdbh == nil {
		return nil, status.UnimplementedError("Suggestions only supported for OLAP DB.")
	}
	q := query_builder.NewQuery("SELECT topK(8, 3)(user) AS top_users, topK(8, 3)(host) AS top_hosts, topK(8, 3)(branch_name) AS top_branches, topK(3)(command) AS top_commands FROM Invocations")
	q.AddWhereClause("group_id = ?", req.GetRequestContext().GetGroupId())
	err := filter.AddGenericFiltersToQuery(q, req.GetFilters(), sfpb.ObjectTypes_INVOCATION_OBJECTS, "clickhouse")
	if err != nil {
		return nil, err
	}

	qStr, qArgs := q.Build()
	rq := s.olapdbh.NewQuery(ctx, "invocation_search_service_filter_suggestions").Raw(qStr, qArgs...)

	type suggestionOutput struct {
		TopUsers    []string `gorm:"type:Array(String);"`
		TopHosts    []string `gorm:"type:Array(String);"`
		TopBranches []string `gorm:"type:Array(String);"`
		TopCommands []string `gorm:"type:Array(String);"`
	}

	out, err := db.ScanAll(rq, &suggestionOutput{})
	if err != nil {
		return nil, err
	}

	suggestions := make([]*sfpb.GenericFilter, 0)
	for _, s := range out {
		for _, u := range s.TopUsers {
			suggestions = append(suggestions, &sfpb.GenericFilter{
				Type:    sfpb.FilterType_USER_FILTER_TYPE,
				Operand: sfpb.FilterOperand_IN_OPERAND,
				Value:   &sfpb.FilterValue{StringValue: []string{u}},
			})
		}
		for _, h := range s.TopHosts {
			suggestions = append(suggestions, &sfpb.GenericFilter{
				Type:    sfpb.FilterType_HOST_FILTER_TYPE,
				Operand: sfpb.FilterOperand_IN_OPERAND,
				Value:   &sfpb.FilterValue{StringValue: []string{h}},
			})
		}
		for _, b := range s.TopBranches {
			suggestions = append(suggestions, &sfpb.GenericFilter{
				Type:    sfpb.FilterType_BRANCH_FILTER_TYPE,
				Operand: sfpb.FilterOperand_IN_OPERAND,
				Value:   &sfpb.FilterValue{StringValue: []string{b}},
			})
		}
		for _, c := range s.TopCommands {
			suggestions = append(suggestions, &sfpb.GenericFilter{
				Type:    sfpb.FilterType_BRANCH_FILTER_TYPE,
				Operand: sfpb.FilterOperand_IN_OPERAND,
				Value:   &sfpb.FilterValue{StringValue: []string{c}},
			})
		}
	}

	return &inpb.GetInvocationFilterSuggestionsResponse{Suggestions: suggestions}, nil
}

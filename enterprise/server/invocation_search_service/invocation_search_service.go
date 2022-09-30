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
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const (
	// See defaultSortParams() for sort defaults.
	defaultLimitSize     = int64(15)
	pageSizeOffsetPrefix = "offset_"
)

type InvocationSearchService struct {
	env environment.Env
	h   interfaces.DBHandle
}

func NewInvocationSearchService(env environment.Env, h interfaces.DBHandle) *InvocationSearchService {
	return &InvocationSearchService{
		env: env,
		h:   h,
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
	q = q.AddWhereClause("("+orQuery+")", orArgs...)
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

	q := query_builder.NewQuery(`SELECT * FROM Invocations as i`)

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

	// Always add permissions check.
	addPermissionsCheckToQuery(u, q)

	sort := defaultSortParams()
	if req.Sort != nil && req.Sort.SortField != inpb.InvocationSort_UNKNOWN_SORT_FIELD {
		sort = req.Sort
	}
	switch sort.SortField {
	case inpb.InvocationSort_CREATED_AT_USEC_SORT_FIELD:
		q.SetOrderBy("created_at_usec", sort.Ascending)
	case inpb.InvocationSort_UPDATED_AT_USEC_SORT_FIELD:
		q.SetOrderBy("updated_at_usec", sort.Ascending)
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

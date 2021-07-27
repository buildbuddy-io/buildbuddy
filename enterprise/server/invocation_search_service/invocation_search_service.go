package invocation_search_service

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_event_handler"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/blocklist"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeutil"
	"github.com/golang/protobuf/ptypes"

	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	tspb "github.com/golang/protobuf/ptypes/timestamp"
)

const (
	// See defaultSortParams() for sort defaults.
	defaultLimitSize     = int64(15)
	pageSizeOffsetPrefix = "offset_"
)

type InvocationSearchService struct {
	env environment.Env
	h   *db.DBHandle
}

func NewInvocationSearchService(env environment.Env, h *db.DBHandle) *InvocationSearchService {
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
	rows, err := s.h.Raw(sql, values...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	invocations := make([]*tables.Invocation, 0)
	for rows.Next() {
		var ti tables.Invocation
		if err := s.h.ScanRows(rows, &ti); err != nil {
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
func addPermissionsCheckToQuery(tu *tables.User, q *query_builder.Query) {
	o := query_builder.OrClauses{}
	o.AddOr("(i.perms & ? != 0)", perms.OTHERS_READ)
	groupArgs := []interface{}{
		perms.GROUP_READ,
	}
	groupParams := make([]string, 0)
	for _, g := range tu.Groups {
		groupArgs = append(groupArgs, g.GroupID)
		groupParams = append(groupParams, "?")
	}
	groupParamString := "(" + strings.Join(groupParams, ", ") + ")"
	groupQueryStr := fmt.Sprintf("(i.perms & ? != 0 AND i.group_id IN %s)", groupParamString)
	o.AddOr(groupQueryStr, groupArgs...)
	o.AddOr("(i.perms & ? != 0 AND i.user_id = ?)", perms.OWNER_READ, tu.UserID)
	orQuery, orArgs := o.Build()
	q = q.AddWhereClause("("+orQuery+")", orArgs...)
}

func (s *InvocationSearchService) QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	if err := s.checkPreconditions(req); err != nil {
		return nil, err
	}
	if s.env.GetUserDB() == nil {
		return nil, status.UnimplementedError("Not implemented.")
	}
	tu, err := s.env.GetUserDB().GetUser(ctx)
	if err != nil {
		// Searching invocations *requires* that you are logged in.
		return nil, err
	}

	groupID := req.GetRequestContext().GetGroupId()
	if err := perms.AuthorizeGroupAccess(ctx, s.env, groupID); err != nil {
		return nil, err
	}
	if blocklist.IsBlockedForStatsQuery(groupID) {
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
	if sha := req.GetQuery().GetCommitSha(); sha != "" {
		q.AddWhereClause("i.commit_sha = ?", sha)
	}
	if group_id := req.GetQuery().GetGroupId(); group_id != "" {
		q.AddWhereClause("i.group_id = ?", group_id)
	}
	if role := req.GetQuery().GetRole(); role != "" {
		q.AddWhereClause("i.role = ?", role)
	}
	if start := req.GetQuery().GetStartTimestamp(); start.GetSeconds() > 0 || start.GetNanos() > 0 {
		q.AddWhereClause("i.created_at_usec >= ?", timestampToMicros(start))
	}
	if end := req.GetQuery().GetEndTimestamp(); end.GetSeconds() > 0 || end.GetNanos() > 0 {
		q.AddWhereClause("i.created_at_usec < ?", timestampToMicros(end))
	}

	// Always add permissions check.
	addPermissionsCheckToQuery(tu, q)

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

func timestampToMicros(tsPb *tspb.Timestamp) int64 {
	ts, _ := ptypes.Timestamp(tsPb)
	return timeutil.ToUsec(ts)
}

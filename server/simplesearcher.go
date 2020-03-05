package simplesearcher

import (
	"context"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/database"
	inpb "proto/invocation"
)

const (
	// See DefaultSortParams() for sort defaults.
	defaultLimitSize = int32(15)
)

func DefaultSortParams() *inpb.InvocationSort {
	return &inpb.InvocationSort{
		SortField: inpb.InvocationSort_CREATED_AT_USEC_SORT_FIELD,
		Ascending: false,
	}
}

type SimpleSearcher struct {
	rawDB *database.Database
}

func NewSimpleSearcher(rawDB *database.Database) *SimpleSearcher {
	return &SimpleSearcher{
		rawDB: rawDB,
	}
}

func (s *SimpleSearcher) IndexInvocation(ctx context.Context, invocation *inpb.Invocation) error {
	// This is a no-op.
	return nil
}

func (s *SimpleSearcher) makeRawQuery(query *inpb.InvocationQuery) (string, []interface{}) {
	qString := `SELECT * FROM Invocations as i`
	whereClauses := make([]string, 0)
	qArgs := make([]interface{}, 0)

	// For now, only search for stuff with status == COMPLETE.
	whereClauses = append(whereClauses, "i.invocation_status = ?")
	qArgs = append(qArgs, int64(inpb.Invocation_COMPLETE_INVOCATION_STATUS))

	if query.User != "" {
		whereClauses = append(whereClauses, "i.user = ?")
		qArgs = append(qArgs, query.User)
	}
	if query.Host != "" {
		whereClauses = append(whereClauses, "i.host = ?")
		qArgs = append(qArgs, query.Host)
	}

	if len(whereClauses) > 0 {
		qString += " WHERE " + strings.Join(whereClauses, " AND ")
	}
	return qString, qArgs
}

func (s *SimpleSearcher) makeSortQuery(reqSort *inpb.InvocationSort) string {
	sortStr := " ORDER BY "
	var sort *inpb.InvocationSort
	if reqSort == nil || reqSort.SortField == inpb.InvocationSort_UNKNOWN_SORT_FIELD {
		sort = DefaultSortParams()
	} else {
		sort = reqSort
	}

	switch sort.SortField {
	case inpb.InvocationSort_CREATED_AT_USEC_SORT_FIELD:
		sortStr += "created_at_usec"
	}

	if sort.Ascending {
		sortStr += " ASC"
	} else {
		sortStr += " DESC"
	}
	return sortStr
}

func (s *SimpleSearcher) checkPreconditions(req *inpb.SearchInvocationRequest) error {
	if req.Query == nil {
		return fmt.Errorf("The query field is required")
	}
	if req.Query.Host == "" && req.Query.User == "" {
		return fmt.Errorf("At least one search atom must be set")
	}
	return nil
}

func (s *SimpleSearcher) QueryInvocations(ctx context.Context, req *inpb.SearchInvocationRequest) (*inpb.SearchInvocationResponse, error) {
	if err := s.checkPreconditions(req); err != nil {
		return nil, err
	}

	qString, qArgs := s.makeRawQuery(req.Query)
	qString += s.makeSortQuery(req.Sort)

	limitSize := defaultLimitSize
	if req.Count > 0 {
		limitSize = req.Count
	}
	qString += fmt.Sprintf(" LIMIT %d", limitSize)

	tableInvocations, err := s.rawDB.RawQueryInvocations(ctx, qString, qArgs...)
	if err != nil {
		return nil, err
	}
	rsp := &inpb.SearchInvocationResponse{}
	for _, ti := range tableInvocations {
		rsp.Invocation = append(rsp.Invocation, ti.ToProto())
	}
	return rsp, nil
}

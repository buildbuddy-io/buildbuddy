package target

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"strings"

	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// PaginationToken is a struct to represent pagination for GetTarget RPC
type PaginationToken struct {
	InvocationEndUsec int64
	CommitSHA         string
}

func NewTokenFromRequest(req *trpb.GetTargetRequest) (*PaginationToken, error) {
	strToken := req.GetPageToken()
	if strToken == "" {
		return nil, nil
	}

	t := &PaginationToken{}
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(strToken))
	if err := gob.NewDecoder(decoder).Decode(t); err != nil {
		return nil, status.InvalidArgumentErrorf("cannot decode page token %q, %s", strToken, err)
	}
	return t, nil
}

func (t *PaginationToken) Encode() (string, error) {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	if err := gob.NewEncoder(encoder).Encode(t); err != nil {
		return "", status.InternalErrorf("failed to tokenize pagination token: %s", err)
	}
	encoder.Close()
	return buf.String(), nil
}

func (t *PaginationToken) ApplyToQuery(q *query_builder.Query) {
	o := query_builder.OrClauses{}
	o.AddOr("created_at_usec < ? ", t.InvocationEndUsec)
	o.AddOr("(created_at_usec = ? AND commit_sha > ?)", t.InvocationEndUsec, t.CommitSHA)
	orQuery, orArgs := o.Build()
	q = q.AddWhereClause("("+orQuery+")", orArgs...)
}

package query_builder_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/stretchr/testify/assert"
)

func TestOrClauses_Empty(t *testing.T) {
	q := query_builder.OrClauses{}

	qStr, qArgs := q.Build()

	assert.Empty(t, qStr, "query string built from empty OrClauses should be empty (no whitespace allowed)")
	assert.Empty(t, qArgs)
}

func TestOrClauses_Single(t *testing.T) {
	q := query_builder.OrClauses{}

	q.AddOr("a = ?", 1)
	qStr, qArgs := q.Build()

	assert.Equal(t, "a = ?", strings.TrimSpace(qStr))
	assert.Equal(t, []interface{}{1}, qArgs)
}

func TestOrClauses_Multiple(t *testing.T) {
	q := query_builder.OrClauses{}

	q.AddOr("a = ?", 1)
	q.AddOr("b = ?", 2)
	qStr, qArgs := q.Build()

	assert.Equal(t, "a = ? OR b = ?", strings.TrimSpace(qStr))
	assert.Equal(t, []interface{}{1, 2}, qArgs)
}

func TestJoinClause(t *testing.T) {
	q := query_builder.NewQuery("SELECT id, t1, t2 FROM targets AS t")

	subQuery := query_builder.NewQuery("SELECT id, i1, i2 FROM invocations AS i")
	subQuery.AddWhereClause("i3 > ?", 4)
	subQuery.AddWhereClause("i4 > ?", 6)
	subQuery.SetOrderBy("i.id", false)
	subQuery.SetLimit(20)

	q.AddJoinClause(subQuery, "s", "f.col1 = s.a")
	q.AddWhereClause("t.t1 > ?", 10)

	qStr, qArgs := q.Build()
	expectedQueryStr := "SELECT id, t1, t2 FROM targets AS t JOIN (SELECT id, i1, i2 FROM invocations AS i WHERE  i3 > ? AND i4 > ?  ORDER BY  i.id DESC LIMIT  20 ) AS s  ON  f.col1 = s.a  WHERE  t.t1 > ?"
	assert.Equal(t, expectedQueryStr, strings.TrimSpace(qStr))
	assert.Equal(t, []interface{}{4, 6, 10}, qArgs)
}

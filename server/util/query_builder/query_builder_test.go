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

func TestJoinInOriginalQuery(t *testing.T) {
	qb := query_builder.NewQuery("SELECT ak.user_id, g.group_id FROM `Groups` AS g, APIKeys AS ak")
	qb.AddWhereClause(`ak.group_id = g.group_id`)
	qb.AddWhereClause(`g.group_id = ?`, "GR1")
	q, args := qb.Build()

	assert.Equal(t, "SELECT ak.user_id, g.group_id FROM `Groups` AS g, APIKeys AS ak WHERE  ak.group_id = g.group_id AND g.group_id = ? ", q)
	assert.Equal(t, []interface{}{"GR1"}, args)
}

func TestFromClause(t *testing.T) {
	q := query_builder.NewQuery("SELECT a")

	subQuery := query_builder.NewQuery("SELECT a, b FROM t")
	subQuery.AddWhereClause("b > ?", 10)
	subQuery.SetOrderBy("a DESC, b", true)
	subQuery.SetLimit(20)

	q.SetFromClause(subQuery)
	q.AddWhereClause("a < ?", 5)
	q.SetLimit(5)
	qStr, qArgs := q.Build()
	expectedQueryStr := "SELECT a FROM (SELECT a, b FROM t WHERE  b > ?  ORDER BY  a DESC, b ASC LIMIT  20 ) WHERE  a < ?  LIMIT  5"
	assert.Equal(t, expectedQueryStr, strings.TrimSpace(qStr))
	assert.Equal(t, []interface{}{10, 5}, qArgs)
}

func TestWhereInClause(t *testing.T) {
	q := query_builder.NewQuery("SELECT a, b, c, d FROM t")
	inListQuery := query_builder.NewQuery("SELECT a")

	innerQuery := query_builder.NewQuery("SELECT a, b FROM t")
	innerQuery.SetOrderBy("a DESC, b", true)
	innerQuery.SetGroupBy("a")

	inListQuery.SetFromClause(innerQuery)
	inListQuery.AddWhereClause("c = ?", 1)
	inListQuery.AddWhereClause("d = ?", 2)
	inListQuery.SetLimit(20)

	q.AddWhereInClause("a", inListQuery)
	q.AddWhereClause("c = ?", 3)
	q.AddWhereClause("d = ?", 4)
	qStr, qArgs := q.Build()
	expectedQueryStr := "SELECT a, b, c, d FROM t WHERE  a IN (SELECT a FROM (SELECT a, b FROM t GROUP BY  a  ORDER BY  a DESC, b ASC) WHERE  c = ? AND d = ?  LIMIT  20 ) AND c = ? AND d = ?"
	assert.Equal(t, expectedQueryStr, strings.TrimSpace(qStr))
	assert.Equal(t, []interface{}{1, 2, 3, 4}, qArgs)
}

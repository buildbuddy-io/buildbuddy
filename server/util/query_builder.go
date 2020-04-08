package query_builder

import (
	"strings"
)

const (
	whereSQLKeyword = "WHERE"
	andQueryJoiner  = "AND"
	orQueryJoiner   = "OR"
)

func pad(clause string) string {
	if clause[0] != ' ' {
		clause = " " + clause
	}
	if clause[len(clause)-1] != ' ' {
		clause = clause + " "
	}
	return clause
}

type Query struct {
	baseQuery    string
	whereClauses []string
	arguments    []interface{}
}

func NewQuery(baseQuery string) *Query {
	return &Query{
		baseQuery:    baseQuery,
		whereClauses: make([]string, 0),
		arguments:    make([]interface{}, 0),
	}
}

func (q *Query) AddWhereClause(clause string, args ...interface{}) *Query {
	clause = pad(clause)
	q.whereClauses = append(q.whereClauses, clause)
	for _, arg := range args {
		q.arguments = append(q.arguments, arg)
	}
	return q
}

func (q *Query) Build() (string, []interface{}) {
	fullQuery := q.baseQuery
	if len(q.whereClauses) > 0 {
		whereRestrict := strings.Join(q.whereClauses, andQueryJoiner)
		fullQuery += pad(whereSQLKeyword) + pad(whereRestrict)
	}
	return fullQuery, q.arguments
}

type OrClauses struct {
	whereClauses []string
	arguments    []interface{}
}

func (o *OrClauses) AddOr(clause string, args ...interface{}) *OrClauses {
	o.whereClauses = append(o.whereClauses, pad(clause))
	for _, arg := range args {
		o.arguments = append(o.arguments, arg)
	}
	return o
}

func (o *OrClauses) Build() (string, []interface{}) {
	fullQuery := ""
	if len(o.whereClauses) > 0 {
		whereRestrict := strings.Join(o.whereClauses, orQueryJoiner)
		fullQuery += pad(whereRestrict)
	}
	return fullQuery, o.arguments
}

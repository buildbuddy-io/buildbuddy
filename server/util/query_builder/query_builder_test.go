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

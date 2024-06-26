package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCaseSensitive(t *testing.T) {
	q, err := NewReQuery("case:y foo", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, "(:eq * foo)")

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "foo")
}

func TestCaseInsensitive(t *testing.T) {
	q, err := NewReQuery("foo", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq * "foo")`)
	assert.Contains(t, squery, `(:eq * "FOO")`)
	assert.Contains(t, squery, `(:eq * "fOo")`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "foo")
	assert.Contains(t, fieldMatchers["content"].String(), "(?mi)")
}

func TestCaseNo(t *testing.T) {
	q, err := NewReQuery("foo case:no", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq * "foo")`)
	assert.Contains(t, squery, `(:eq * "FOO")`)
	assert.Contains(t, squery, `(:eq * "fOo")`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "foo")
	assert.Contains(t, fieldMatchers["content"].String(), "(?mi)")
}

func TestLangAtom(t *testing.T) {
	q, err := NewReQuery("lang:java foo", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, "(:eq lang \"java\")")

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "foo")
}

func TestFileAtom(t *testing.T) {
	q, err := NewReQuery("f:foo/bar/baz.a", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, "(:eq filename \"/ba\")")
	assert.Contains(t, squery, "(:eq filename \"ar/\")")
	assert.Contains(t, squery, "(:eq filename \"bar\")")
	assert.Contains(t, squery, "(:eq filename \"baz\")")
	assert.Contains(t, squery, "(:eq filename \"foo\")")
	assert.Contains(t, squery, "(:eq filename \"o/b\")")
	assert.Contains(t, squery, "(:eq filename \"oo/\")")
	assert.Contains(t, squery, "(:eq filename \"r/b\")")

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.NotContains(t, fieldMatchers, "content")
}

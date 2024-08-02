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
	q, err := NewReQuery("Foo", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq * foo)`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "Foo")
	assert.Contains(t, fieldMatchers["content"].String(), "(?mi)")
}

func TestCaseNo(t *testing.T) {
	q, err := NewReQuery("fOO case:no", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq * foo)`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "fOO")
	assert.Contains(t, fieldMatchers["content"].String(), "(?mi)")
}

func TestLangAtom(t *testing.T) {
	q, err := NewReQuery("lang:java foo", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, "(:eq language \"java\")")

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

func TestGroupedTerms(t *testing.T) {
	q, err := NewReQuery(`"grp trm" case:y`, 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq * "grp")`)
	assert.Contains(t, squery, `(:eq * "trm")`)
	assert.Contains(t, squery, `(:eq * "p t")`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "(grp trm)")
}

func TestUngroupedTerms(t *testing.T) {
	q, err := NewReQuery("grp trm case:y", 1)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:and (:eq * grp) (:eq * trm))`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "(grp)|(trm)")
}

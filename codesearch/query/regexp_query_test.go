package query

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCaseSensitive(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "case:y foo")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq content "foo")`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].re.String(), "foo")
}

func TestCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "Foo")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq content "foo")`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	assert.Len(t, fieldMatchers, 1)
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "Foo")
	assert.Contains(t, fieldMatchers["content"].String(), "(?mi)")
}

func TestCaseNo(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "fOO case:no")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq content "foo")`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "fOO")
	assert.Contains(t, fieldMatchers["content"].String(), "(?mi)")
}

func TestLangAtom(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "lang:java foo")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, "(:eq language \"java\")")

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "foo")
}

func TestLangAtomOnly(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "lang:java")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:and  (:eq language "java"))`)

	// No field matchers should be created for language filters - the index handles it
	assert.Empty(t, q.TestOnlyFieldMatchers())
}

func TestFileAtom(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "f:foo/bar/baz.a")
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
	ctx := context.Background()
	q, err := NewReQuery(ctx, `"grp trm" case:y`)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq content "grp")`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "(grp trm)")
}

func TestUngroupedTerms(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "grp trm case:y")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:and (:eq content "grp") (:eq content "trm"))`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "content")
	assert.Contains(t, fieldMatchers["content"].String(), "(grp)|(trm)")
}

func TestFilenameFilter(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "file:index")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:and (:eq filename "dex") (:eq filename "ind") (:eq filename "nde"))`)

	fieldMatchers := q.TestOnlyFieldMatchers()
	require.Contains(t, fieldMatchers, "filename")
	assert.Contains(t, fieldMatchers["filename"].String(), "index")
}

func TestRepoFilter(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "repo:cats")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:and  (:eq repo "cats"))`)

	// No field matchers should be created for repo filters - the index handles it
	assert.Empty(t, q.TestOnlyFieldMatchers())
}

// define schema
// make test doc
// call score on individual docs

var testSchema = schema.NewDocumentSchema(
	[]types.FieldSchema{
		schema.MustFieldSchema(types.KeywordField, "id", true),
		schema.MustFieldSchema(types.TrigramField, "filename", true),
		schema.MustFieldSchema(types.SparseNgramField, "content", true),
	},
)

func newTestDocument(t *testing.T, fieldMap map[string][]byte) types.Document {
	doc, err := testSchema.MakeDocument(fieldMap)
	if err != nil {
		t.Fatalf("failed to create test document: %v", err)
	}
	return doc
}

func TestScoringSingleMatch(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "foo")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	doc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("foo"),
	})

	score := scorer.Score(nil, doc)
	assert.Equal(t, 1.0, score)
}

func TestScorerWithNoMatchers(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "lang:java")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:and  (:eq language "java"))`)

	// make some docs and assert scores are 1.0
}

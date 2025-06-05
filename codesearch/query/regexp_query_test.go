package query

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testSchema = schema.NewDocumentSchema(
	[]types.FieldSchema{
		schema.MustFieldSchema(types.KeywordField, "id", true),
		schema.MustFieldSchema(types.TrigramField, "filename", true),
		schema.MustFieldSchema(types.SparseNgramField, "content", true),
		schema.MustFieldSchema(types.KeywordField, "lang", true),
	},
)

func newTestDocument(t *testing.T, fieldMap map[string][]byte) types.Document {
	doc, err := testSchema.MakeDocument(fieldMap)
	if err != nil {
		t.Fatalf("failed to create test document: %v", err)
	}
	return doc
}

func TestCaseSensitive(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "case:y foo")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq content "foo")`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "foo")
}

func TestCaseInsensitive(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "Foo")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq content "foo")`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "Foo")
	assert.Contains(t, contentMatcher.String(), "(?mi)")
}

func TestCaseNo(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "fOO case:no")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq content "foo")`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "fOO")
	assert.Contains(t, contentMatcher.String(), "(?mi)")
}

func TestLangAtom(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "lang:java foo")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, "(:eq language \"java\")")
	assert.Contains(t, squery, `(:eq content "foo")`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "foo")
}

func TestLangAtomOnly(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "lang:java")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Equal(t, squery, `(:eq language "java")`)

	// No field matchers should be created for language filters - the index handles it
	assert.Nil(t, q.TestOnlyContentMatcher())
}

func TestRepoAtom(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "repo:cats foo")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq repo "cats")`)
	assert.Contains(t, squery, `(:eq content "foo")`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "foo")
}

func TestRepoAtomOnly(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "repo:cats")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Equal(t, squery, `(:eq repo "cats")`)

	// No field matchers should be created for repo filters - the index handles it
	assert.Nil(t, q.TestOnlyContentMatcher())

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

	assert.Nil(t, q.TestOnlyContentMatcher())
}

func TestFileAndContentAtoms(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "f:bar.go foo")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq filename "bar")`)
	assert.Contains(t, squery, `(:eq content "foo")`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "foo")
}

func TestGroupedTerms(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, `"grp trm" case:y`)
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:eq content "grp")`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "(grp trm)")
}

func TestUngroupedTerms(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "grp trm case:y")
	require.NoError(t, err)

	squery := string(q.SQuery())
	assert.Contains(t, squery, `(:and (:or (:eq content "grp") (:eq filename "grp")) (:or (:eq content "trm") (:eq filename "trm")))`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "(grp)|(trm)")
}

func TestScoringMatchContentOnly(t *testing.T) {
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
	require.NotNil(t, score)
	assert.InDelta(t, 0.9, score, 0.1)
}

func TestScoringMatchContentAndFilename(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "foo f:bar.go")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	doc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.go"),
		"content":  []byte("foo"),
	})

	score := scorer.Score(nil, doc)
	require.NotNil(t, score)
	// A very coarse check: filename matches the filename match should contribute 1, and
	// the content match should contribute between 0 and 1.
	assert.InDelta(t, 0.9, score, 0.1)
}

func TestScoringMatchFilenameWithoutAtom(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "bar")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	doc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("foo"),
	})

	score := scorer.Score(nil, doc)
	require.NotNil(t, score)
	// A very coarse check: the filename match should contribute between 0 and 1.
	assert.InDelta(t, 0.55, score, 0.1)
}

func TestScoringMatchExplicitFilename(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "file:bar")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	doc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("foo"),
	})

	score := scorer.Score(nil, doc)
	require.NotNil(t, score)
	assert.InDelta(t, 1.0, score, 0.01)
}

func TestScorerWithNoMatchers(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "lang:java")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	doc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("foo"),
		"lang":     []byte("java"),
	})

	score := scorer.Score(nil, doc)
	require.NotNil(t, score)
	assert.Equal(t, 1.0, score)
}

func TestScorerNonMatch(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "baz")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	doc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("foo"),
	})

	score := scorer.Score(nil, doc)
	require.NotNil(t, score)
	assert.Equal(t, 0.0, score)
}

func TestScorerWithOnlyOneMatch(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "filepath:baz foo")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	doc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("foo"),
	})

	score := scorer.Score(nil, doc)
	require.NotNil(t, score)
	assert.Equal(t, 0.0, score)
}

func TestScorerWithShortFilePathNoMatch(t *testing.T) {
	// A short token - fewer than 3 characters - will match everything in the trigram
	// index, so we must ensure that the scorer will score non-matches as 0 so they are filtered out
	// later.
	ctx := context.Background()
	q, err := NewReQuery(ctx, "filepath:zo foo")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	doc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("foo"),
	})

	score := scorer.Score(nil, doc)
	require.NotNil(t, score)
	assert.Equal(t, 0.0, score)
}

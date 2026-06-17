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
	},
)

func newTestDocument(t *testing.T, fieldMap map[string][]byte) types.Document {
	doc, err := testSchema.MakeDocument(fieldMap)
	if err != nil {
		t.Fatalf("failed to create test document: %v", err)
	}
	return doc
}

type testPosting struct {
	docid     uint64
	frequency uint32
}

func (p testPosting) Docid() uint64 {
	return p.docid
}

func (p testPosting) Frequency() uint32 {
	return p.frequency
}

type testDocumentMatch struct {
	docid        uint64
	postings     map[string]types.Posting
	fieldLengths map[string]uint32
}

func (m testDocumentMatch) Docid() uint64 {
	return m.docid
}

func (m testDocumentMatch) FieldNames() []string {
	fields := make([]string, 0, len(m.postings))
	for fieldName := range m.postings {
		fields = append(fields, fieldName)
	}
	return fields
}

func (m testDocumentMatch) Posting(fieldName string) types.Posting {
	return m.postings[fieldName]
}

func (m testDocumentMatch) FieldLength(fieldName string) uint32 {
	return m.fieldLengths[fieldName]
}

func (m testDocumentMatch) Signal(name string) float64 {
	return 0
}

func matchWithFrequencies(freqs map[string]uint32) types.DocumentMatch {
	return matchWithFrequenciesAndLengths(freqs, freqs)
}

func matchWithFrequenciesAndLengths(freqs, fieldLengths map[string]uint32) types.DocumentMatch {
	postings := make(map[string]types.Posting, len(freqs))
	for fieldName, frequency := range freqs {
		postings[fieldName] = testPosting{docid: 1, frequency: frequency}
	}
	return testDocumentMatch{docid: 1, postings: postings, fieldLengths: fieldLengths}
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
	// Each bare-identifier term also matches the symbols field (scoring
	// evidence only; it doesn't narrow the candidate set).
	assert.Contains(t, squery, `(:and (:or (:eq content "grp") (:eq filename "grp") (:eq symbols "grp")) (:or (:eq content "trm") (:eq filename "trm") (:eq symbols "trm")))`)

	contentMatcher := q.TestOnlyContentMatcher()
	assert.NotNil(t, contentMatcher)
	assert.Contains(t, contentMatcher.String(), "(grp)|(trm)")
}

func TestSymbolsClauseOnlyForIdentifiers(t *testing.T) {
	ctx := context.Background()

	// A bare identifier additionally matches the symbols field, lowercased to
	// match the keyword tokenizer.
	q, err := NewReQuery(ctx, "Println")
	require.NoError(t, err)
	assert.Contains(t, string(q.SQuery()), `(:eq symbols "println")`)

	// A term with regex metacharacters isn't a symbol name, so no symbols
	// clause is added.
	q, err = NewReQuery(ctx, "Pri.tln")
	require.NoError(t, err)
	assert.NotContains(t, string(q.SQuery()), "(:eq symbols")
}

func TestScoringMatchContentOnly(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "foo")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	docMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	// content contributes weight(2)*sat(tf(1)); filename has no posting.
	// Unprepared (no candidate stats), field norms default to 1.
	assert.Equal(t, 2*bm25Sat(1), scorer.Score(docMatch))
}

func TestNestedAndGatingPreserved(t *testing.T) {
	// An And nested under an Or must keep its zero-gating: a doc matching
	// only one of the And's children contributes nothing, not a partial
	// score. Guards against gating regressing if it lived only in Score.
	nestedAnd := andScorers(
		newFieldScorer(contentField, 1, nil, true),
		newFieldScorer(filenameField, 1, nil, true),
	)
	scorer := orScorers(nestedAnd, newFieldScorer("other", 1, nil, true))

	// Matches contentField only; the And's filename child is absent, and the
	// Or's other child is absent too, so the whole expression scores 0.
	docMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	assert.Equal(t, 0.0, scorer.Score(docMatch))

	// Matching both And children does contribute.
	bothMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1, filenameField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	assert.Greater(t, scorer.Score(bothMatch), 0.0)
}

func TestScoringLengthNormalization(t *testing.T) {
	// Two docs with the same match count but different lengths: after
	// Prepare computes the candidate-set average, each doc's field is scored
	// by its length relative to that field's average (BM25 dl/avgdl), so the
	// shorter doc scores higher but is not rewarded in proportion to raw
	// length.
	ctx := context.Background()
	q, err := NewReQuery(ctx, "foo")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	short := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	long := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		map[string]uint32{contentField: 4, filenameField: 1},
	)
	scorer.Prepare([]types.DocumentMatch{short, long}) // avg content len = 2.5

	// Each doc's content tf is normalized by its content length relative
	// to the candidate average: B = 1 - b + b*(len/avg).
	shortNorm := 1 - bm25B + bm25B*(1.0/2.5)
	longNorm := 1 - bm25B + bm25B*(4.0/2.5)
	assert.InDelta(t, 2*bm25Sat(1.0/shortNorm), scorer.Score(short), 1e-9)
	assert.InDelta(t, 2*bm25Sat(1.0/longNorm), scorer.Score(long), 1e-9)
	assert.Greater(t, scorer.Score(short), scorer.Score(long))
}

func TestScoringMissingFieldLengthsFloorsAtMatchCount(t *testing.T) {
	// Docs indexed before field lengths were stored report FieldLength 0 for
	// every field. The scorer should fall back to the match count as the field
	// length rather than scoring with length normalization disabled.
	ctx := context.Background()
	q, err := NewReQuery(ctx, "foo")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	noLengths := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		nil,
	)
	withLengths := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		map[string]uint32{contentField: 4},
	)
	scorer.Prepare([]types.DocumentMatch{noLengths, withLengths}) // avg content len = 4

	// The lengthless doc's content length floors at its tf (1), not 0.
	flooredNorm := 1 - bm25B + bm25B*(1.0/4.0)
	assert.InDelta(t, 2*bm25Sat(1.0/flooredNorm), scorer.Score(noLengths), 1e-9)
}

func TestScoringMatchContentAndFilename(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "foo f:bar.go")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	docMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{
			contentField:  1,
			filenameField: 1,
		},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	// Root is And(filename-filter, content-or); fields saturate
	// independently and sum: filter 2*sat(1), content 2*sat(1), implicit
	// filename 1*sat(1).
	assert.Equal(t, 5*bm25Sat(1), scorer.Score(docMatch))
}

func TestScoringMatchFilenameWithoutAtom(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "bar")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	docMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{filenameField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	// Implicit filename match only: weight(1)*sat(tf(1)).
	assert.Equal(t, 1*bm25Sat(1), scorer.Score(docMatch))
}

func TestScoringMatchExplicitFilename(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "file:bar")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	docMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{filenameField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	// Explicit filename filter: weight(2)*sat(tf(1)).
	assert.Equal(t, 2*bm25Sat(1), scorer.Score(docMatch))
}

func TestScorerWithNoMatchers(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "lang:java")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	assert.Equal(t, 1.0, scorer.Score(matchWithFrequencies(nil)))
}

func TestScorerNonMatch(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "baz")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	docMatch := matchWithFrequenciesAndLengths(
		nil,
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	assert.Equal(t, 0.0, scorer.Score(docMatch))
}

func TestScorerWithOnlyOneMatch(t *testing.T) {
	ctx := context.Background()
	q, err := NewReQuery(ctx, "filepath:baz foo")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	docMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	assert.Equal(t, 0.0, scorer.Score(docMatch))
}

func TestScorerWithShortFilePathDefersToRescore(t *testing.T) {
	// A short filename pattern - fewer than 3 characters - produces no
	// trigrams, so the index cannot filter candidates by it. The cheap scorer
	// must not veto such docs (it has no evidence either way); Rescore checks
	// the real filename and separates matches from non-matches.
	ctx := context.Background()
	q, err := NewReQuery(ctx, "filepath:zo foo")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	docMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	assert.Greater(t, scorer.Score(docMatch), 0.0)

	nonMatchingDoc := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("foo"),
	})
	assert.Equal(t, 0.0, scorer.Rescore(docMatch, nonMatchingDoc))

	matchingDoc := newTestDocument(t, map[string][]byte{
		"id":       []byte("2"),
		"filename": []byte("zoo.txt"),
		"content":  []byte("foo"),
	})
	assert.Greater(t, scorer.Rescore(docMatch, matchingDoc), 0.0)
}

func TestRescoreDropsTrigramFalsePositive(t *testing.T) {
	// A doc can carry all of a term's ngrams without containing the term;
	// Rescore runs the real matcher over the stored content and rejects it.
	ctx := context.Background()
	q, err := NewReQuery(ctx, "fields")
	require.NoError(t, err)

	scorer := q.Scorer()
	require.NotNil(t, scorer)

	docMatch := matchWithFrequenciesAndLengths(
		map[string]uint32{contentField: 1},
		map[string]uint32{contentField: 1, filenameField: 1},
	)
	assert.Greater(t, scorer.Score(docMatch), 0.0)

	falsePositive := newTestDocument(t, map[string][]byte{
		"id":       []byte("1"),
		"filename": []byte("bar.txt"),
		"content":  []byte("fie elder ldside"), // ngrams without the term
	})
	assert.Equal(t, 0.0, scorer.Rescore(docMatch, falsePositive))

	realMatch := newTestDocument(t, map[string][]byte{
		"id":       []byte("2"),
		"filename": []byte("baz.txt"),
		"content":  []byte("func Fields(s string)"),
	})
	assert.Greater(t, scorer.Rescore(docMatch, realMatch), 0.0)
}

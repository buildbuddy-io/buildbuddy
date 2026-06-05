package searcher_test

import (
	"context"
	"sort"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testSchema = schema.NewDocumentSchema(
	[]types.FieldSchema{
		schema.MustFieldSchema(types.KeywordField, "ident", true),
		schema.MustFieldSchema(types.SparseNgramField, "content", true),
	})

func makeTestDoc(ident, content string) types.Document {
	doc, err := testSchema.MakeDocument(map[string][]byte{
		"ident":   []byte(ident),
		"content": []byte(content),
	})
	if err != nil {
		panic(err)
	}
	return doc
}

var sampleData = []struct {
	id      string
	content string
}{
	{"one", "one is the loneliest number"},
	{"two", "two times two"},
	{"three", "three body problem"},
	{"four", "four score and"},
	{"five", "hawaii five-o"},
	{"six", "pick up sticks is great"},
	{"seven", "lucky number seven"},
	{"eight", "pieces of eight"},
	{"nine", "nine lives"},
	{"ten", "ten things i hate about you"},
	{"eleven", "turn it up to 11"},
}

type constantScorer struct{}

func (s constantScorer) Skip() bool { return false }
func (s constantScorer) UpperBoundScore(docMatch types.DocumentMatch) float64 {
	return 0.1
}
func (s constantScorer) Score(docMatch types.DocumentMatch, doc types.Document) float64 {
	return 0.1
}

type explicitScorer struct {
	upperBounds map[uint64]float64
	scores      map[uint64]float64
	mu          sync.Mutex
	exactCalls  int
}

func (s *explicitScorer) Skip() bool { return false }
func (s *explicitScorer) UpperBoundScore(docMatch types.DocumentMatch) float64 {
	if score, ok := s.upperBounds[docMatch.Docid()]; ok {
		return score
	}
	if s.upperBounds != nil {
		return 1.0
	}
	if score, ok := s.scores[docMatch.Docid()]; ok {
		return score
	}
	return 0.0
}
func (s *explicitScorer) Score(docMatch types.DocumentMatch, doc types.Document) float64 {
	s.mu.Lock()
	s.exactCalls++
	s.mu.Unlock()
	if score, ok := s.scores[docMatch.Docid()]; ok {
		return score
	}
	return 0.0
}

func (s *explicitScorer) ExactCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exactCalls
}

type sQuery struct {
	s      string
	scorer types.Scorer
}

func (q sQuery) SQuery() string       { return q.s }
func (q sQuery) Scorer() types.Scorer { return q.scorer }

func createSampleIndex(t testing.TB) *pebble.DB {
	t.Helper()

	indexDir := testfs.MakeTempDir(t)
	db, err := index.OpenPebbleDB(indexDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		db.Close()
	})

	w, err := index.NewWriter(db, "testns")
	if err != nil {
		t.Fatal(err)
	}
	for _, doc := range sampleData {
		require.NoError(t, w.AddDocument(makeTestDoc(doc.id, doc.content)))
	}
	require.NoError(t, w.Flush())
	return db
}

func TestBasicSearcher(t *testing.T) {
	ctx := context.Background()
	db := createSampleIndex(t)
	s := searcher.New(ctx, index.NewReader(ctx, db, "testns", testSchema))
	docs, err := s.Search(sQuery{"(:all)", constantScorer{}}, 100, 0)
	require.NoError(t, err)
	require.Equal(t, len(sampleData), len(docs))
}

func TestSearcherOffsetAndLimit(t *testing.T) {
	ctx := context.Background()
	db := createSampleIndex(t)
	s := searcher.New(ctx, index.NewReader(ctx, db, "testns", testSchema))
	docs, err := s.Search(sQuery{"(:all)", constantScorer{}}, 11, 8)
	require.NoError(t, err)
	require.Equal(t, 3, len(docs))

	assert.Equal(t, "nine", string(docs[0].Field("ident").Contents()))
	assert.Equal(t, "ten", string(docs[1].Field("ident").Contents()))
	assert.Equal(t, "eleven", string(docs[2].Field("ident").Contents()))
}

func TestSearcherZeroScoresDropped(t *testing.T) {
	ctx := context.Background()
	db := createSampleIndex(t)
	s := searcher.New(ctx, index.NewReader(ctx, db, "testns", testSchema))

	scorer := &explicitScorer{
		scores: map[uint64]float64{
			1: 1.0,
			4: 0.5,
			8: 0.00001,
		},
	}
	docs, err := s.Search(sQuery{"(:all)", scorer}, 100, 0)
	require.NoError(t, err)
	require.Equal(t, 3, len(docs))

	assert.Equal(t, "one", string(docs[0].Field("ident").Contents()))
	assert.Equal(t, "four", string(docs[1].Field("ident").Contents()))
	assert.Equal(t, "eight", string(docs[2].Field("ident").Contents()))
}

func TestSearcherStopsWhenUpperBoundsCannotEnterTopK(t *testing.T) {
	ctx := context.Background()
	db := createSampleIndex(t)
	s := searcher.New(ctx, index.NewReader(ctx, db, "testns", testSchema))

	scorer := &explicitScorer{
		upperBounds: map[uint64]float64{
			1: 100.0,
			2: 90.0,
		},
		scores: map[uint64]float64{
			1: 100.0,
			2: 90.0,
		},
	}
	docs, err := s.Search(sQuery{"(:all)", scorer}, 2, 0)
	require.NoError(t, err)
	require.Equal(t, 2, len(docs))

	assert.Equal(t, "one", string(docs[0].Field("ident").Contents()))
	assert.Equal(t, "two", string(docs[1].Field("ident").Contents()))
	assert.Equal(t, 2, scorer.ExactCalls())
}

func TestSearcherTopKMatchesExhaustiveScanWithLooseUpperBounds(t *testing.T) {
	ctx := context.Background()
	db := createSampleIndex(t)
	s := searcher.New(ctx, index.NewReader(ctx, db, "testns", testSchema))

	scorer := &explicitScorer{
		upperBounds: map[uint64]float64{
			1:  100.0,
			2:  95.0,
			3:  90.0,
			4:  85.0,
			5:  84.0,
			6:  83.0,
			7:  82.0,
			8:  81.0,
			9:  10.0,
			10: 8.0,
			11: 7.0,
		},
		scores: map[uint64]float64{
			1:  1.0,
			2:  80.0,
			3:  2.0,
			4:  85.0,
			5:  3.0,
			6:  4.0,
			7:  5.0,
			8:  81.0,
			9:  9.0,
			10: 8.0,
			11: 7.0,
		},
	}
	for docID, score := range scorer.scores {
		require.GreaterOrEqual(t, scorer.upperBounds[docID], score)
	}
	docs, err := s.Search(sQuery{"(:all)", scorer}, 3, 0)
	require.NoError(t, err)

	assert.Equal(t, exhaustiveTopIdents(scorer.scores, 3, 0), docIdents(docs))
	assert.Less(t, scorer.ExactCalls(), len(sampleData))
}

func TestSearcherEvictsLargerDocIDOnScoreTie(t *testing.T) {
	ctx := context.Background()
	db := createSampleIndex(t)
	s := searcher.New(ctx, index.NewReader(ctx, db, "testns", testSchema))

	scorer := &explicitScorer{
		upperBounds: map[uint64]float64{
			1: 100.0,
			2: 100.0,
			3: 100.0,
		},
		scores: map[uint64]float64{
			1: 1.0,
			2: 1.0,
			3: 1.0,
		},
	}
	docs, err := s.Search(sQuery{"(:all)", scorer}, 2, 0)
	require.NoError(t, err)

	assert.Equal(t, []string{"one", "two"}, docIdents(docs))
}

func docIdents(docs []types.Document) []string {
	idents := make([]string, 0, len(docs))
	for _, doc := range docs {
		idents = append(idents, string(doc.Field("ident").Contents()))
	}
	return idents
}

func exhaustiveTopIdents(scores map[uint64]float64, numResults, offset int) []string {
	docIDs := make([]uint64, 0, len(scores))
	for docID, score := range scores {
		if score > 0 {
			docIDs = append(docIDs, docID)
		}
	}
	sort.Slice(docIDs, func(i, j int) bool {
		if scores[docIDs[i]] == scores[docIDs[j]] {
			return docIDs[i] < docIDs[j]
		}
		return scores[docIDs[i]] > scores[docIDs[j]]
	})

	start := min(offset, len(docIDs))
	end := min(offset+numResults, len(docIDs))
	idents := make([]string, 0, end-start)
	for _, docID := range docIDs[start:end] {
		idents = append(idents, sampleData[docID-1].id)
	}
	return idents
}

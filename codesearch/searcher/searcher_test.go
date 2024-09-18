package searcher_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/searcher"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func makeTestDoc(ident, content string) types.Document {
	doc := types.NewMapDocument(
		map[string]types.NamedField{
			"ident":   types.NewNamedField(types.KeywordField, "ident", []byte(ident), true /*=stored*/),
			"content": types.NewNamedField(types.SparseNgramField, "content", []byte(content), true /*=stored*/),
		},
	)
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
	{"six", "pick up sticks"},
	{"seven", "lucky number seven"},
	{"eight", "pieces of eight"},
	{"nine", "nine lives"},
	{"ten", "ten things i hate about you"},
	{"eleven", "turn it up to 11"},
}

type zeroScorer struct{}

func (s zeroScorer) Skip() bool                                                     { return false }
func (s zeroScorer) Score(docMatch types.DocumentMatch, doc types.Document) float64 { return 0.0 }

type sQuery struct {
	s      string
	scorer types.Scorer
}

func (q sQuery) SQuery() string       { return q.s }
func (q sQuery) Scorer() types.Scorer { return q.scorer }

func createSampleIndex(t testing.TB) *pebble.DB {
	t.Helper()

	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
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
	s := searcher.New(ctx, index.NewReader(ctx, db, "testns"))
	docs, err := s.Search(sQuery{"(:all)", zeroScorer{}}, 100, 0)
	require.NoError(t, err)
	require.Equal(t, len(sampleData), len(docs))
}

func TestSearcherOffsetAndLimit(t *testing.T) {
	ctx := context.Background()
	db := createSampleIndex(t)
	s := searcher.New(ctx, index.NewReader(ctx, db, "testns"))
	docs, err := s.Search(sQuery{"(:all)", zeroScorer{}}, 11, 8)
	require.NoError(t, err)
	require.Equal(t, 3, len(docs))

	assert.Equal(t, "nine", string(docs[0].Field("ident").Contents()))
	assert.Equal(t, "ten", string(docs[1].Field("ident").Contents()))
	assert.Equal(t, "eleven", string(docs[2].Field("ident").Contents()))
}

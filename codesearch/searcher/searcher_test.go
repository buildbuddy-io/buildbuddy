package searcher_test

import (
	"context"
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
	{"six", "pick up sticks"},
	{"seven", "lucky number seven"},
	{"eight", "pieces of eight"},
	{"nine", "nine lives"},
	{"ten", "ten things i hate about you"},
	{"eleven", "turn it up to 11"},
}

type constantScorer struct{}

func (s constantScorer) Skip() bool                                                     { return false }
func (s constantScorer) Score(docMatch types.DocumentMatch, doc types.Document) float64 { return 0.1 }

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

// test search for short tokens alone
// test search for short tokens combined with other tokens, where short tokens match
// test search for short tokens combined with other tokens, where short token doesn't always match any docs - e.g. "foo is" or "ind.z"

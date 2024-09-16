package index

import (
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

type TestDocument struct {
	id     uint64
	fields map[string]types.NamedField
}

func (d TestDocument) ID() uint64                    { return d.id }
func (d TestDocument) Field(name string) types.Field { return d.fields[name] }
func (d TestDocument) Fields() []string {
	fieldNames := make([]string, 0, len(d.fields))
	for name := range d.fields {
		fieldNames = append(fieldNames, name)
	}
	return fieldNames
}
func NewTestDocument(id uint64, fieldMap map[string]types.NamedField) TestDocument {
	return TestDocument{id, fieldMap}
}

func docWithName(name string) types.Document {
	return NewTestDocument(
		hash(name),
		map[string]types.NamedField{
			name: types.NewNamedField(types.TrigramField, name, []byte(name), true /*=stored*/),
		},
	)
}

func docWithID(id uint64) types.Document {
	return NewTestDocument(
		id,
		map[string]types.NamedField{
			"id": types.NewNamedField(types.StringTokenField, "id", []byte(fmt.Sprintf("%d", id)), true /*=stored*/),
		},
	)
}

func docWithIDAndText(id uint64, text string) types.Document {
	return NewTestDocument(
		id,
		map[string]types.NamedField{
			"id":   types.NewNamedField(types.StringTokenField, "id", []byte(fmt.Sprintf("%d", id)), true /*=stored*/),
			"text": types.NewNamedField(types.TrigramField, "text", []byte(text), true /*=stored*/),
		},
	)
}

func extractFieldMatches(tb testing.TB, r types.IndexReader, docMatches []types.DocumentMatch) map[string][]uint64 {
	tb.Helper()
	m := make(map[string][]uint64)
	for _, docMatch := range docMatches {
		storedDoc, err := r.GetStoredDocument(docMatch.Docid())
		require.NoError(tb, err)
		id, err := strconv.ParseUint(string(storedDoc.Field("id").Contents()), 10, 64)
		require.NoError(tb, err)
		for _, fieldName := range docMatch.FieldNames() {
			m[fieldName] = append(m[fieldName], id)
		}
	}

	for _, v := range m {
		slices.Sort(v)
	}
	return m
}

func TestInvalidFieldNames(t *testing.T) {
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w, err := NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	assert.Error(t, w.AddDocument(docWithName("_bad_field_name")))
	assert.Error(t, w.AddDocument(docWithName(".bad_field_name")))
	assert.Error(t, w.AddDocument(docWithName("bad field name")))

	assert.NoError(t, w.AddDocument(docWithName("good_field_name")))
	assert.NoError(t, w.AddDocument(docWithName("good_field_name12345")))
	assert.NoError(t, w.AddDocument(docWithName("1good_field_name")))
}

func TestDeletes(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w, err := NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(docWithID(1)))
	require.NoError(t, w.AddDocument(docWithID(2)))
	require.NoError(t, w.AddDocument(docWithID(3)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace")
	matches, err := r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {1, 2, 3}}, extractFieldMatches(t, r, matches))

	// Delete a doc
	w, err = NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	w.DeleteDocument(2)
	require.NoError(t, w.Flush())

	r = NewReader(ctx, db, "testing-namespace")
	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {1, 3}}, extractFieldMatches(t, r, matches))
}

func TestIncrementalIndexing(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	w, err := NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(docWithIDAndText(1, `one foo`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(2, `two bar`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(3, `three baz`)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace")
	matches, err := r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1}}, extractFieldMatches(t, r, matches))

	// Now add some docs and delete others and ensure the returned results
	// are as expected.
	w, err = NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.DeleteDocument(1))
	require.NoError(t, w.AddDocument(docWithIDAndText(4, `four bap`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(5, `one zip`)))
	require.NoError(t, w.Flush())

	r = NewReader(ctx, db, "testing-namespace")
	matches, err = r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {5}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {2, 3, 4, 5}, "id": {2, 3, 4, 5}}, extractFieldMatches(t, r, matches))
}

func TestUnknownTokenType(t *testing.T) {
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w, err := NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	doc := NewTestDocument(
		1,
		map[string]types.NamedField{
			"name": types.NewNamedField(types.FieldType(99), "name", []byte("name"), true /*=stored*/),
		},
	)
	assert.Error(t, w.AddDocument(doc))
}

func TestStoredVsUnstoredFields(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w, err := NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	doc := NewTestDocument(
		1,
		map[string]types.NamedField{
			"id":      types.NewNamedField(types.StringTokenField, "id", []byte("1"), true /*=stored*/),
			"field_a": types.NewNamedField(types.StringTokenField, "field_a", []byte("stored"), true /*=stored*/),
			"field_b": types.NewNamedField(types.StringTokenField, "field_b", []byte("unstored"), false /*=stored*/),
		},
	)
	assert.NoError(t, w.AddDocument(doc))
	require.NoError(t, w.Flush())

	// docs should be searchable by stored fields
	r := NewReader(ctx, db, "testing-namespace")
	matches, err := r.RawQuery(`(:eq field_a stored)`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"field_a": {1}}, extractFieldMatches(t, r, matches))

	// docs should be searchable by non-stored fields
	matches, err = r.RawQuery(`(:eq field_b unstored)`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"field_b": {1}}, extractFieldMatches(t, r, matches))

	// docs should be searchable by both stored and non-stored fields
	matches, err = r.RawQuery(`(:or (:eq field_b unstored) (:eq field_a stored))`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"field_b": {1}, "field_a": {1}}, extractFieldMatches(t, r, matches))

	// stored document should only contain stored fields
	rdoc, err := r.GetStoredDocument(1)
	require.NoError(t, err)
	require.Equal(t, []byte("stored"), rdoc.Field("field_a").Contents())
	require.Nil(t, rdoc.Field("field_b").Contents())
}

func TestNamespaceSeparation(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w, err := NewWriter(db, "namespace-a")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(docWithIDAndText(1, `one foo`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(2, `two bar`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(3, `three baz`)))
	require.NoError(t, w.Flush())

	w, err = NewWriter(db, "namespace-b")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(docWithIDAndText(1, `one oof`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(2, `two rab`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(3, `three zab`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(4, `four pab`)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "namespace-a")
	matches, err := r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {1, 2, 3}, "text": {1, 2, 3}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1}}, extractFieldMatches(t, r, matches))

	r = NewReader(ctx, db, "namespace-b")
	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {1, 2, 3, 4}, "text": {1, 2, 3, 4}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text pab)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {4}}, extractFieldMatches(t, r, matches))
}

func TestSQuery(t *testing.T) {
	ctx := context.Background()
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	w, err := NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(docWithIDAndText(1, `one foo`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(2, `two bar`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(3, `three baz`)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace")
	matches, err := r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {1, 2, 3}, "text": {1, 2, 3}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:none)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:or (:eq text one) (:eq text bar))")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1, 2}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text \" ba\")")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {2, 3}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:and (:eq text \" ba\") (:eq text two))")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {2}}, extractFieldMatches(t, r, matches))

	_, err = r.RawQuery("(:and (:)") // invalid q
	require.Error(t, err)
}

func printDB(t testing.TB, db *pebble.DB) {
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0},
		UpperBound: []byte{255},
	})
	require.NoError(t, err)
	defer iter.Close()
	log.Printf("<BEGIN DB>")
	for iter.First(); iter.Valid(); iter.Next() {
		log.Printf("\tkey: %q %x [%d]", string(iter.Key()), iter.Value(), len(iter.Value()))
	}
	log.Printf("<END DB>")
}

func TestDBFormat(t *testing.T) {
	indexDir := testfs.MakeTempDir(t)
	db, err := pebble.Open(indexDir, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	doc1 := NewTestDocument(
		1,
		map[string]types.NamedField{
			"id":      types.NewNamedField(types.StringTokenField, "id", []byte("1"), true /*=stored*/),
			"content": types.NewNamedField(types.StringTokenField, "content", []byte("one"), false /*=stored*/),
		},
	)
	doc2 := NewTestDocument(
		2,
		map[string]types.NamedField{
			"id":      types.NewNamedField(types.StringTokenField, "id", []byte("2"), true /*=stored*/),
			"content": types.NewNamedField(types.StringTokenField, "content", []byte("two"), false /*=stored*/),
		},
	)

	w, err := NewWriter(db, "testns")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(doc1))
	require.NoError(t, w.AddDocument(doc2))
	require.NoError(t, w.Flush())

	// Re-add doc1 again.
	doc1 = NewTestDocument(
		1,
		map[string]types.NamedField{
			"id":      types.NewNamedField(types.StringTokenField, "id", []byte("1"), true /*=stored*/),
			"content": types.NewNamedField(types.StringTokenField, "content", []byte("ONE"), false /*=stored*/),
		},
	)
	w, err = NewWriter(db, "testns")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(doc1))
	require.NoError(t, w.Flush())

	printDB(t, db)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0},
		UpperBound: []byte{255},
	})
	require.NoError(t, err)
	defer iter.Close()

	require.True(t, iter.First())

	require.Equal(t, "__generation__", string(iter.Key())) // global segment generation key
	require.Equal(t, uint32(1), BytesToUint32(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:1:_id", string(iter.Key())) // first doc ptr
	require.Equal(t, uint64(1), BytesToUint64(iter.Value())) // 1st segment, 1st docid

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:1:id", string(iter.Key())) // first doc ptr
	require.Equal(t, "1", string(iter.Value()))             // 2nd segment, 1st docid

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:2:_id", string(iter.Key())) // second doc ptr
	require.Equal(t, uint64(2), BytesToUint64(iter.Value())) // 1st segment, 2nd docid

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:2:id", string(iter.Key())) // second doc id field content
	require.Equal(t, "2", string(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:4294967297:_id", string(iter.Key())) // first doc id field content
	require.Equal(t, uint64(4294967297), BytesToUint64(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:4294967297:id", string(iter.Key())) // first doc id field content
	require.Equal(t, "1", string(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:1:id", string(iter.Key())) // ngram "1", field "id" posting list
	pl1ID, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	require.Equal(t, []uint64{1, uint64(1<<32) + 1}, pl1ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:2:id", string(iter.Key())) // ngram "2", field "id" posting list
	pl2ID, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, pl2ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:one:content", string(iter.Key())) // ngram "one", field content PL
	plOneContent, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	require.Equal(t, []uint64{1, uint64(1<<32) + 1}, plOneContent.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:two:content", string(iter.Key())) // ngram "two", field content PL
	plTwoContent, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, plTwoContent.ToArray())

	require.False(t, iter.Next()) // End of data.

}

// WRITE:
// Test new index size is ~equal~ to old index size?
// Benchmark index performance?

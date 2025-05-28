package index

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testSchema = schema.NewDocumentSchema(
	[]types.FieldSchema{
		schema.MustFieldSchema(types.KeywordField, "id", true),
		schema.MustFieldSchema(types.TrigramField, "text", true),
	},
)

func newTestDocument(t *testing.T, fieldMap map[string][]byte) types.Document {
	doc, err := testSchema.MakeDocument(fieldMap)
	if err != nil {
		t.Fatalf("failed to create test document: %v", err)
	}
	return doc
}

func docWithID(t *testing.T, id uint64) types.Document {
	return newTestDocument(
		t,
		map[string][]byte{
			"id": []byte(fmt.Sprintf("%d", id)),
		},
	)
}

func docWithIDAndText(t *testing.T, id uint64, text string) types.Document {
	return newTestDocument(
		t,
		map[string][]byte{
			"id":   []byte(fmt.Sprintf("%d", id)),
			"text": []byte(text),
		},
	)
}

func extractFieldMatches(tb testing.TB, r types.IndexReader, docMatches []types.DocumentMatch) map[string][]uint64 {
	tb.Helper()
	m := make(map[string][]uint64)
	for _, docMatch := range docMatches {
		storedDoc := r.GetStoredDocument(docMatch.Docid())
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

func mustOpenDB(t *testing.T, indexDir string) *pebble.DB {
	t.Helper()
	db, err := OpenPebbleDB(indexDir)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestDeletes(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	require.NoError(t, w.AddDocument(docWithID(t, 1)))
	require.NoError(t, w.AddDocument(docWithID(t, 2)))
	require.NoError(t, w.AddDocument(docWithID(t, 3)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", testSchema)
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

	r = NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {1, 3}}, extractFieldMatches(t, r, matches))
}

func TestIncrementalIndexing(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 1, `one foo`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 2, `two bar`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 3, `three baz`)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err := r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1}}, extractFieldMatches(t, r, matches))

	// Now add some docs and delete others and ensure the returned results
	// are as expected.
	w, err = NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	doc1 := docWithIDAndText(t, 1, `one one one`)
	require.NoError(t, w.UpdateDocument(doc1.Field("id"), doc1))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 4, `four bap`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 5, `one zip`)))
	require.NoError(t, w.Flush())

	r = NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err = r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1, 5}}, extractFieldMatches(t, r, matches))

	// Update the same document 5 times in a row and ensure it's still
	// only returned once below.
	for i := 0; i < 5; i++ {
		w, err = NewWriter(db, "testing-namespace")
		if err != nil {
			t.Fatal(err)
		}
		require.NoError(t, w.UpdateDocument(doc1.Field("id"), doc1))
		require.NoError(t, w.Flush())
	}

	printDB(t, db)
	r = NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err = r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1, 5}}, extractFieldMatches(t, r, matches))
}

func TestUpdateSameDocTwiceInSameBatch(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 7, `one one one`)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err := r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {7}}, extractFieldMatches(t, r, matches))

	w, err = NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}

	// Update the document twice in the same batch, make sure the last update sticks
	docV2 := docWithIDAndText(t, 7, `two two two`)
	docV3 := docWithIDAndText(t, 7, `three three three`)
	require.NoError(t, w.UpdateDocument(docV2.Field("id"), docV2))
	require.NoError(t, w.UpdateDocument(docV3.Field("id"), docV3))
	require.NoError(t, w.Flush())

	r = NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err = r.RawQuery("(:eq text thr)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {7}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text two)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, extractFieldMatches(t, r, matches))
}

func TestUpdateSameDocThriceInSameBatch(t *testing.T) {
	// This test hits unique cases that "update twice" doesn't hit, related to
	// updating the doc id / match field mappins when a document is updated multiple
	// time in the same batch.
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 7, `one one one`)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err := r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {7}}, extractFieldMatches(t, r, matches))

	w, err = NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	docV2 := docWithIDAndText(t, 7, `two two two`)
	docV3 := docWithIDAndText(t, 7, `three three three`)
	docV4 := docWithIDAndText(t, 7, `four four four`)
	require.NoError(t, w.UpdateDocument(docV2.Field("id"), docV2))
	require.NoError(t, w.UpdateDocument(docV3.Field("id"), docV3))
	require.NoError(t, w.UpdateDocument(docV4.Field("id"), docV4))
	require.NoError(t, w.Flush())

	r = NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err = r.RawQuery("(:eq text fou)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {7}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text thr)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text two)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, extractFieldMatches(t, r, matches))
}

func TestStoredVsUnstoredFields(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.KeywordField, "field_a", true),
			schema.MustFieldSchema(types.KeywordField, "field_b", false),
		},
	)
	doc, err := docSchema.MakeDocument(
		map[string][]byte{
			"id":      []byte("7"),
			"field_a": []byte("stored"),
			"field_b": []byte("unstored"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	assert.NoError(t, w.AddDocument(doc))
	require.NoError(t, w.Flush())

	// docs should be searchable by stored fields
	r := NewReader(ctx, db, "testing-namespace", docSchema)
	matches, err := r.RawQuery(`(:eq field_a stored)`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"field_a": {7}}, extractFieldMatches(t, r, matches))

	// docs should be searchable by non-stored fields
	matches, err = r.RawQuery(`(:eq field_b unstored)`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"field_b": {7}}, extractFieldMatches(t, r, matches))

	// docs should be searchable by both stored and non-stored fields
	matches, err = r.RawQuery(`(:or (:eq field_b unstored) (:eq field_a stored))`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"field_b": {7}, "field_a": {7}}, extractFieldMatches(t, r, matches))

	// stored document should only contain stored fields
	rdoc := r.GetStoredDocument(1)
	assert.Equal(t, []byte("7"), rdoc.Field("id").Contents())
	assert.Equal(t, []byte("stored"), rdoc.Field("field_a").Contents())
	assert.Nil(t, rdoc.Field("field_b").Contents())
}

func TestGetStoredDocument(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.KeywordField, "field_a", true),
		},
	)
	doc, err := docSchema.MakeDocument(
		map[string][]byte{
			"id":      []byte("50"),
			"field_a": []byte("stored"),
		},
	)
	require.NoError(t, err)

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	assert.NoError(t, w.AddDocument(doc))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", docSchema)

	// stored document should only contain stored fields
	rdoc := r.GetStoredDocument(1)
	assert.Equal(t, []byte("50"), rdoc.Field("id").Contents())
	assert.Equal(t, []byte("stored"), rdoc.Field("field_a").Contents())
}

func TestNamespaceSeparation(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "namespace-a")
	require.NoError(t, err)

	require.NoError(t, w.AddDocument(docWithIDAndText(t, 1, `one foo`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 2, `two bar`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 3, `three baz`)))
	require.NoError(t, w.Flush())

	w, err = NewWriter(db, "namespace-b")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 1, `one oof`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 2, `two rab`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 3, `three zab`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 4, `four pab`)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "namespace-a", testSchema)
	matches, err := r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {1, 2, 3}, "text": {1, 2, 3}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1}}, extractFieldMatches(t, r, matches))

	r = NewReader(ctx, db, "namespace-b", testSchema)
	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {1, 2, 3, 4}, "text": {1, 2, 3, 4}}, extractFieldMatches(t, r, matches))

	matches, err = r.RawQuery("(:eq text pab)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {4}}, extractFieldMatches(t, r, matches))
}

func TestSQuery(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	require.NoError(t, w.AddDocument(docWithIDAndText(t, 1, `one foo`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 2, `two bar`)))
	require.NoError(t, w.AddDocument(docWithIDAndText(t, 3, `three baz`)))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", testSchema)
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

func TestMetadataDocs(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	commitSHA := "abc123"
	repoURL := "github.com/buildbuddy-io/buildbuddy"

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	fields := map[string][]byte{
		schema.IDField:        []byte(repoURL),
		schema.LatestSHAField: []byte(commitSHA),
	}

	doc, err := schema.MetadataSchema().MakeDocument(fields)
	require.NoError(t, err)

	require.NoError(t, w.UpdateDocument(doc.Field(schema.IDField), doc))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", schema.MetadataSchema())
	readDoc := r.GetStoredDocument(1)
	assert.Equal(t, commitSHA, string(readDoc.Field(schema.LatestSHAField).Contents()))
}

func TestCompactDeletes(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	doc := docWithIDAndText(t, 8, `one`)
	require.NoError(t, w.AddDocument(doc))
	require.NoError(t, w.Flush())

	printDB(t, db)

	r := NewReader(ctx, db, "testing-namespace", testSchema)
	delList, err := r.postingList([]byte(types.DeletesField), posting.NewFieldMap(), types.DeletesField)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), delList.GetCardinality())

	w, err = NewWriter(db, "testing-namespace")
	require.NoError(t, err)
	require.NoError(t, w.UpdateDocument(doc.Field("id"), doc))
	require.NoError(t, w.Flush())

	printDB(t, db)

	r = NewReader(ctx, db, "testing-namespace", testSchema)
	delList, err = r.postingList([]byte(types.DeletesField), posting.NewFieldMap(), types.DeletesField)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), delList.GetCardinality())
	assert.Equal(t, []uint64{1}, delList.ToPosting().ToArray())

	oneList, err := r.postingList([]byte("one"), posting.NewFieldMap(), "text")
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, (1<<32 | 1)}, oneList.ToPosting().ToArray())

	printDB(t, db)

	w, err = NewWriter(db, "testing-namespace")
	require.NoError(t, err)
	require.NoError(t, w.CompactDeletes())
	require.NoError(t, w.Flush())

	r = NewReader(ctx, db, "testing-namespace", testSchema)
	delList, err = r.postingList([]byte(types.DeletesField), posting.NewFieldMap(), types.DeletesField)
	require.NoError(t, err)
	assert.Equal(t, []uint64{}, delList.ToPosting().ToArray())

	oneList, err = r.postingList([]byte("one"), posting.NewFieldMap(), "text")
	require.NoError(t, err)
	assert.Equal(t, []uint64{1<<32 | 1}, oneList.ToPosting().ToArray())
	printDB(t, db)
}

func TestDeleteMatchingDocuments(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	schema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.TrigramField, "text", true),
			schema.MustFieldSchema(types.KeywordField, "url", true),
		},
	)

	doc1 := schema.MustMakeDocument(map[string][]byte{
		"id":   []byte("1"),
		"text": []byte("one"),
		"url":  []byte("github.com/buildbuddy-io/buildbuddy"),
	})
	doc2 := schema.MustMakeDocument(map[string][]byte{
		"id":   []byte("2"),
		"text": []byte("one"),
		"url":  []byte("github.com/buildbuddy-io/buildbuddy-internal"),
	})

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	require.NoError(t, w.AddDocument(doc1))
	require.NoError(t, w.AddDocument(doc2))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err := r.RawQuery(`(:eq text one)`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1, 2}}, extractFieldMatches(t, r, matches))

	w, err = NewWriter(db, "testing-namespace")
	require.NoError(t, err)
	require.NoError(t, w.DeleteMatchingDocuments(schema.Field("url").MakeField([]byte("github.com/buildbuddy-io/buildbuddy"))))
	require.NoError(t, w.Flush())

	r = NewReader(ctx, db, "testing-namespace", testSchema)
	matches, err = r.RawQuery(`(:eq text one)`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {2}}, extractFieldMatches(t, r, matches))
}

func TestDropNamespace(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "ns-a")
	require.NoError(t, err)

	doc := docWithIDAndText(t, 8, `one`)
	require.NoError(t, w.AddDocument(doc))
	require.NoError(t, w.Flush())

	w, err = NewWriter(db, "ns-b")
	require.NoError(t, err)

	doc2 := docWithIDAndText(t, 9, `one`)
	require.NoError(t, w.AddDocument(doc2))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "ns-a", testSchema)
	matches, err := r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {8}, "text": {8}}, extractFieldMatches(t, r, matches))

	r = NewReader(ctx, db, "ns-b", testSchema)
	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {9}, "text": {9}}, extractFieldMatches(t, r, matches))

	w, err = NewWriter(db, "ns-a")
	require.NoError(t, err)
	require.NoError(t, w.DropNamespace())
	require.NoError(t, w.Flush())

	r = NewReader(ctx, db, "ns-a", testSchema)
	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, extractFieldMatches(t, r, matches))

	r = NewReader(ctx, db, "ns-b", testSchema)
	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"id": {9}, "text": {9}}, extractFieldMatches(t, r, matches))

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
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.KeywordField, "content", false),
		},
	)
	doc1, err := docSchema.MakeDocument(
		map[string][]byte{
			"id":      []byte("1"),
			"content": []byte("one"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	doc2, err := docSchema.MakeDocument(
		map[string][]byte{
			"id":      []byte("2"),
			"content": []byte("two"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	w, err := NewWriter(db, "testns")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.AddDocument(doc1))
	require.NoError(t, w.AddDocument(doc2))
	require.NoError(t, w.Flush())

	// Re-add doc1 again.
	doc1, err = docSchema.MakeDocument(
		map[string][]byte{
			"id":      []byte("1"),
			"content": []byte("ONE"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	w, err = NewWriter(db, "testns")
	if err != nil {
		t.Fatal(err)
	}
	require.NoError(t, w.UpdateDocument(doc1.Field("id"), doc1))
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
	require.Equal(t, []uint64{uint64(1<<32) + 1}, pl1ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:2:id", string(iter.Key())) // ngram "2", field "id" posting list
	pl2ID, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	require.Equal(t, []uint64{2}, pl2ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:_del:_del", string(iter.Key()))
	plDel, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, plDel.ToArray()) // doc 1 was deleted (via UpdateDocument)

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

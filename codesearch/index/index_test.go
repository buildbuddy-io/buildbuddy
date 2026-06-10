package index

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/performance"
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

func requireFieldLengths(t testing.TB, value []byte, want map[string]uint32) {
	t.Helper()
	got, err := unmarshalFieldLengths(value)
	require.NoError(t, err)
	assert.Equal(t, want, got)
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
	// This test adds a document, updates it, checks that the original document was deleted,
	// then compacts the deletes and checks to ensure that the deleted document id is fully removed.
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "testing-namespace")
	require.NoError(t, err)

	doc := docWithIDAndText(t, 8, `one`)
	require.NoError(t, w.AddDocument(doc))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testing-namespace", testSchema)
	delList, err := r.postingList([]byte(types.DeletesField), posting.NewFieldMap(), types.DeletesField)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), delList.GetCardinality())

	w, err = NewWriter(db, "testing-namespace")
	require.NoError(t, err)
	require.NoError(t, w.UpdateDocument(doc.Field("id"), doc))
	require.NoError(t, w.Flush())

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

func TestMergeActuallyMerges(t *testing.T) {
	// This test ensures that our custom merger is actually merging posting lists when updating,
	// rather than just appending them (which is what would happen if the default merger was used).
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	w, err := NewWriter(db, "ns-a")
	require.NoError(t, err)

	doc := docWithIDAndText(t, 8, `one`)
	require.NoError(t, w.AddDocument(doc))
	require.NoError(t, w.Flush())

	w, err = NewWriter(db, "ns-a")
	require.NoError(t, err)

	doc2 := docWithIDAndText(t, 9, `done`) // should end up in pl for "one" also
	require.NoError(t, w.AddDocument(doc2))
	require.NoError(t, w.Flush())

	plBytes, closer, err := w.db.Get([]byte("ns-a:gra:one:text"))
	require.NoError(t, err)
	t.Cleanup(func() { closer.Close() })

	expectedBytes, err := posting.NewList(1, (1<<32 | 1)).Marshal()
	require.NoError(t, err)

	assert.Equal(t, expectedBytes, plBytes)
}

func TestSparseNgramFrequenciesArePersisted(t *testing.T) {
	db := mustOpenDB(t, testfs.MakeTempDir(t))
	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.SparseNgramField, "content", true),
		},
	)
	doc := docSchema.MustMakeDocument(
		map[string][]byte{
			"id":      []byte("1"),
			"content": []byte("abc abc abc"),
		},
	)

	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(doc))
	require.NoError(t, w.Flush())

	plBytes, closer, err := db.Get(postingListKey("testns", "abc", "content"))
	require.NoError(t, err)
	defer closer.Close()

	pl, err := posting.Unmarshal(plBytes)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, pl.ToArray())
	assert.Equal(t, uint32(3), pl.Frequency(1))
}

func TestSparseNgramFrequenciesAcrossMultipleDocs(t *testing.T) {
	db := mustOpenDB(t, testfs.MakeTempDir(t))
	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.SparseNgramField, "content", true),
		},
	)

	// Index three docs with different per-doc frequencies for the same ngram.
	// This also exercises the pebble merger path, since each AddDocument's
	// posting list is merged into the on-disk value.
	docs := []struct {
		id      string
		content string
		wantTF  uint32
	}{
		{"1", "abc", 1},
		{"2", "abc abc abc", 3},
		{"3", "abc abc abc abc abc", 5},
	}
	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	for _, d := range docs {
		require.NoError(t, w.AddDocument(docSchema.MustMakeDocument(map[string][]byte{
			"id":      []byte(d.id),
			"content": []byte(d.content),
		})))
	}
	require.NoError(t, w.Flush())

	plBytes, closer, err := db.Get(postingListKey("testns", "abc", "content"))
	require.NoError(t, err)
	defer closer.Close()

	pl, err := posting.Unmarshal(plBytes)
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, pl.ToArray())
	for i, d := range docs {
		docID := uint64(i + 1)
		assert.Equal(t, d.wantTF, pl.Frequency(docID), "docID=%d (%s)", docID, d.id)
	}

	// Round-trip through UnmarshalReadOnly too, which is the path the query
	// engine takes against pebble-owned memory.
	roList, err := posting.UnmarshalReadOnly(plBytes)
	require.NoError(t, err)
	for i, d := range docs {
		docID := uint64(i + 1)
		assert.Equal(t, d.wantTF, roList.Frequency(docID), "readonly docID=%d (%s)", docID, d.id)
	}
}

type tfEntry struct {
	docID uint64
	freq  uint32
}

func marshalPostings(t *testing.T, entries []tfEntry) []byte {
	t.Helper()
	pl := posting.NewMergeList()
	for _, e := range entries {
		pl.AddWithFrequency(e.docID, e.freq)
	}
	buf, err := pl.Marshal()
	require.NoError(t, err)
	return buf
}

// mergeValues runs operands through the pebble merger the same way pebble
// does: the first operand seeds the merger, the rest arrive via MergeNewer.
func mergeValues(t *testing.T, first []byte, rest ...[]byte) []byte {
	t.Helper()
	m, err := initRoaringMerger([]byte("testkey"), first)
	require.NoError(t, err)
	for _, v := range rest {
		require.NoError(t, m.MergeNewer(v))
	}
	out, closer, err := m.Finish(true)
	require.NoError(t, err)
	if closer != nil {
		defer closer.Close()
	}
	return out
}

func requireFrequencies(t *testing.T, plBytes []byte, want []tfEntry) {
	t.Helper()
	wantIDs := make([]uint64, len(want))
	for i, e := range want {
		wantIDs[i] = e.docID
	}

	pl, err := posting.Unmarshal(plBytes)
	require.NoError(t, err)
	assert.Equal(t, wantIDs, pl.ToArray())
	for _, e := range want {
		assert.Equal(t, e.freq, pl.Frequency(e.docID), "docID=%d", e.docID)
	}

	// Also decode through the no-copy path the query engine uses.
	roList, err := posting.UnmarshalReadOnly(plBytes)
	require.NoError(t, err)
	for _, e := range want {
		assert.Equal(t, e.freq, roList.Frequency(e.docID), "readonly docID=%d", e.docID)
	}
}

func TestMergerPreservesDisjointTFPayloads(t *testing.T) {
	// The multi-writer scenario: two writers index disjoint doc sets (distinct
	// generations), and both flush counted posting lists for the same ngram.
	gen0 := marshalPostings(t, []tfEntry{{1, 3}, {2, 1}})
	gen1 := marshalPostings(t, []tfEntry{{1<<32 | 1, 5}, {1<<32 | 2, 2}})

	merged := mergeValues(t, gen0, gen1)
	requireFrequencies(t, merged, []tfEntry{
		{1, 3},
		{2, 1},
		{1<<32 | 1, 5},
		{1<<32 | 2, 2},
	})
}

func TestMergerSumsTFForSharedDocs(t *testing.T) {
	a := marshalPostings(t, []tfEntry{{1, 3}, {2, 1}})
	b := marshalPostings(t, []tfEntry{{1, 2}, {3, 4}})

	merged := mergeValues(t, a, b)
	requireFrequencies(t, merged, []tfEntry{{1, 5}, {2, 1}, {3, 4}})
}

func TestMergerCountedWithPlain(t *testing.T) {
	counted := marshalPostings(t, []tfEntry{{1, 7}})
	plain, err := posting.NewList(2).Marshal()
	require.NoError(t, err)

	want := []tfEntry{{1, 7}, {2, 1}}
	// Both operand orders: a plain list merging into a counted base and vice
	// versa. Under concurrent writers either can arrive first.
	requireFrequencies(t, mergeValues(t, counted, plain), want)
	requireFrequencies(t, mergeValues(t, plain, counted), want)

	// A plain list sharing a doc with the counted list contributes frequency 1
	// to that doc's sum.
	overlapping, err := posting.NewList(1, 2).Marshal()
	require.NoError(t, err)
	wantSummed := []tfEntry{{1, 8}, {2, 1}}
	requireFrequencies(t, mergeValues(t, counted, overlapping), wantSummed)
	requireFrequencies(t, mergeValues(t, overlapping, counted), wantSummed)
}

func TestMergerAllOnesStaysPlain(t *testing.T) {
	// Lists whose frequencies are all 1 must keep serializing as plain roaring
	// bytes (no CSTF header) so merged values stay readable by older readers.
	a, err := posting.NewList(1, 2).Marshal()
	require.NoError(t, err)
	b, err := posting.NewList(1<<32|1, 1<<32|2).Marshal()
	require.NoError(t, err)

	merged := mergeValues(t, a, b)
	assert.False(t, bytes.HasPrefix(merged, []byte("CSTF")))
	requireFrequencies(t, merged, []tfEntry{
		{1, 1},
		{2, 1},
		{1<<32 | 1, 1},
		{1<<32 | 2, 1},
	})
}

func TestMergerOrderIndependence(t *testing.T) {
	// Pebble may feed operands oldest-first (MergeOlder) or newest-first
	// (MergeNewer) depending on whether the merge happens during compaction or
	// a read. TF merging must commute.
	a := marshalPostings(t, []tfEntry{{1, 3}, {2, 1}})
	b := marshalPostings(t, []tfEntry{{1, 2}, {1<<32 | 1, 5}})

	newerFirst := mergeValues(t, a, b)

	m, err := initRoaringMerger([]byte("testkey"), b)
	require.NoError(t, err)
	require.NoError(t, m.MergeOlder(a))
	olderFirst, closer, err := m.Finish(true)
	require.NoError(t, err)
	if closer != nil {
		defer closer.Close()
	}

	assert.Equal(t, newerFirst, olderFirst)
	requireFrequencies(t, newerFirst, []tfEntry{{1, 5}, {2, 1}, {1<<32 | 1, 5}})
}

func TestMergerEmptyBase(t *testing.T) {
	counted := marshalPostings(t, []tfEntry{{1, 4}})
	merged := mergeValues(t, nil, counted)
	requireFrequencies(t, merged, []tfEntry{{1, 4}})
}

func TestTFPayloadsSurviveMultiWriterFlush(t *testing.T) {
	// End-to-end version of TestMergerPreservesDisjointTFPayloads: two writers
	// (which get distinct generations) interleave adds and flushes against the
	// same namespace, so pebble's merger combines their counted posting lists
	// for the shared ngram key.
	db := mustOpenDB(t, testfs.MakeTempDir(t))
	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.SparseNgramField, "content", true),
		},
	)

	w1, err := NewWriter(db, "testns")
	require.NoError(t, err)
	w2, err := NewWriter(db, "testns")
	require.NoError(t, err)

	require.NoError(t, w1.AddDocument(docSchema.MustMakeDocument(map[string][]byte{
		"id":      []byte("1"),
		"content": []byte("abc abc abc"),
	})))
	require.NoError(t, w2.AddDocument(docSchema.MustMakeDocument(map[string][]byte{
		"id":      []byte("2"),
		"content": []byte("abc abc abc abc abc"),
	})))
	require.NoError(t, w1.Flush())
	require.NoError(t, w2.Flush())

	plBytes, closer, err := db.Get(postingListKey("testns", "abc", "content"))
	require.NoError(t, err)
	defer closer.Close()

	// Writer doc IDs are generation<<32 | docIndex; the writers were created in
	// order, so w1 is generation 0 and w2 is generation 1.
	requireFrequencies(t, plBytes, []tfEntry{
		{1, 3},
		{1<<32 | 1, 5},
	})
}

func TestRawQueryDocumentMatchesIncludeFrequencies(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))
	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.SparseNgramField, "content", true),
		},
	)

	docs := []struct {
		id      string
		content string
		wantTF  uint32
	}{
		{"1", "abc", 1},
		{"2", "abc abc abc", 3},
	}
	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	for _, d := range docs {
		require.NoError(t, w.AddDocument(docSchema.MustMakeDocument(map[string][]byte{
			"id":      []byte(d.id),
			"content": []byte(d.content),
		})))
	}
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testns", docSchema)
	matches, err := r.RawQuery(`(:eq content abc)`)
	require.NoError(t, err)

	got := make(map[string]uint32)
	for _, match := range matches {
		storedDoc := r.GetStoredDocument(match.Docid())
		id := string(storedDoc.Field("id").Contents())
		posting := match.Posting("content")
		require.NotNil(t, posting)
		got[id] = posting.Frequency()
	}
	assert.Equal(t, map[string]uint32{"1": 1, "2": 3}, got)
}

func TestRawQueryDocumentMatchesIncludeFieldLengths(t *testing.T) {
	ctx := context.Background()
	db := mustOpenDB(t, testfs.MakeTempDir(t))
	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.KeywordField, "content", true),
		},
	)

	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(docSchema.MustMakeDocument(map[string][]byte{
		"id":      []byte("1"),
		"content": []byte("one two two"),
	})))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testns", docSchema)
	matches, err := r.RawQuery(`(:eq content two)`)
	require.NoError(t, err)
	require.Len(t, matches, 1)

	assert.Equal(t, uint32(1), matches[0].FieldLength("id"))
	assert.Equal(t, uint32(3), matches[0].FieldLength("content"))
}

func TestLazyDocFieldSeeksRequestedField(t *testing.T) {
	ctx := performance.WrapContext(context.Background())
	db := mustOpenDB(t, testfs.MakeTempDir(t))
	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.KeywordField, "content", true),
			schema.MustFieldSchema(types.KeywordField, "owner", true),
		},
	)

	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(docSchema.MustMakeDocument(map[string][]byte{
		"id":      []byte("1"),
		"content": []byte("needle"),
		"owner":   []byte("tyler"),
	})))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testns", docSchema)
	storedDoc := r.GetStoredDocument(1)
	assert.Equal(t, "needle", string(storedDoc.Field("content").Contents()))

	tracker := performance.TrackerFromContext(ctx)
	require.NotNil(t, tracker)
	assert.Equal(t, int64(1), tracker.Get(performance.DOC_KEYS_SCANNED))
}

func TestGetStoredFieldsSeeksRequestedFields(t *testing.T) {
	ctx := performance.WrapContext(context.Background())
	db := mustOpenDB(t, testfs.MakeTempDir(t))
	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.KeywordField, "content", true),
			schema.MustFieldSchema(types.KeywordField, "owner", true),
		},
	)

	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(docSchema.MustMakeDocument(map[string][]byte{
		"id":      []byte("1"),
		"content": []byte("needle"),
		"owner":   []byte("tyler"),
	})))
	require.NoError(t, w.Flush())

	r := NewReader(ctx, db, "testns", docSchema)
	fields, err := r.getStoredFields(1, "owner", "content")
	require.NoError(t, err)
	assert.Equal(t, "needle", string(fields["content"].Contents()))
	assert.Equal(t, "tyler", string(fields["owner"].Contents()))

	tracker := performance.TrackerFromContext(ctx)
	require.NotNil(t, tracker)
	assert.Equal(t, int64(2), tracker.Get(performance.DOC_KEYS_SCANNED))
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

func TestDBFormatAddOnly(t *testing.T) {
	// This test ensures that the expected key/value pairs are written to the database when documents
	// are added.
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.KeywordField, "content", false),
		},
	)
	doc1 := docSchema.MustMakeDocument(
		map[string][]byte{
			"id":      []byte("1"),
			"content": []byte("one"),
		},
	)
	doc2 := docSchema.MustMakeDocument(
		map[string][]byte{
			"id":      []byte("2"),
			"content": []byte("two"),
		},
	)

	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(doc1))
	require.NoError(t, w.AddDocument(doc2))
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
	assert.Equal(t, uint32(0), BytesToUint32(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:1:_id", string(iter.Key())) // first doc id field content
	assert.Equal(t, uint64(1), BytesToUint64(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:1:id", string(iter.Key())) // first doc id field content
	assert.Equal(t, "1", string(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:2:_id", string(iter.Key())) // second doc ptr
	assert.Equal(t, uint64(2), BytesToUint64(iter.Value()))  // 1st segment, 2nd docid

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:2:id", string(iter.Key())) // second doc id field content
	assert.Equal(t, "2", string(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:1:id", string(iter.Key())) // ngram "1", field "id" posting list
	pl1ID, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, pl1ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:2:id", string(iter.Key())) // ngram "2", field "id" posting list
	pl2ID, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{2}, pl2ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:one:content", string(iter.Key())) // ngram "one", field content PL
	plOneContent, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, plOneContent.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:two:content", string(iter.Key())) // ngram "two", field content PL
	plTwoContent, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{2}, plTwoContent.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:sta:1:_field_lengths", string(iter.Key()))
	requireFieldLengths(t, iter.Value(), map[string]uint32{"id": 1, "content": 1})

	require.True(t, iter.Next())
	require.Equal(t, "testns:sta:2:_field_lengths", string(iter.Key()))
	requireFieldLengths(t, iter.Value(), map[string]uint32{"id": 1, "content": 1})

	assert.False(t, iter.Next()) // End of data.
}

func TestDBFormatUpdate(t *testing.T) {
	// This test ensures that the expected key/value pairs are written to the database when a
	// document is updated.

	db := mustOpenDB(t, testfs.MakeTempDir(t))

	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.KeywordField, "content", false),
		},
	)
	doc1 := docSchema.MustMakeDocument(
		map[string][]byte{
			"id":      []byte("1"),
			"content": []byte("one"),
		},
	)
	doc2 := docSchema.MustMakeDocument(
		map[string][]byte{
			"id":      []byte("2"),
			"content": []byte("two"),
		},
	)

	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(doc1))
	require.NoError(t, w.AddDocument(doc2))
	require.NoError(t, w.Flush())

	// Re-add doc1 again.
	doc1 = docSchema.MustMakeDocument(
		map[string][]byte{
			"id":      []byte("1"),
			"content": []byte("ONE"),
		},
	)

	w, err = NewWriter(db, "testns")
	require.NoError(t, err)
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
	assert.Equal(t, uint32(1), BytesToUint32(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:2:_id", string(iter.Key())) // second doc ptr
	assert.Equal(t, uint64(2), BytesToUint64(iter.Value()))  // 1st segment, 2nd docid

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:2:id", string(iter.Key())) // second doc id field content
	assert.Equal(t, "2", string(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:4294967297:_id", string(iter.Key())) // first doc id field content
	assert.Equal(t, uint64(4294967297), BytesToUint64(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:4294967297:id", string(iter.Key())) // first doc id field content
	assert.Equal(t, "1", string(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:1:id", string(iter.Key())) // ngram "1", field "id" posting list
	pl1ID, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{uint64(1<<32) + 1}, pl1ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:2:id", string(iter.Key())) // ngram "2", field "id" posting list
	pl2ID, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{2}, pl2ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:_del:_del", string(iter.Key()))
	plDel, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, plDel.ToArray()) // doc 1 was deleted (via UpdateDocument)

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:one:content", string(iter.Key())) // ngram "one", field content PL
	plOneContent, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, uint64(1<<32) + 1}, plOneContent.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:two:content", string(iter.Key())) // ngram "two", field content PL
	plTwoContent, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{2}, plTwoContent.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:sta:2:_field_lengths", string(iter.Key()))
	requireFieldLengths(t, iter.Value(), map[string]uint32{"id": 1, "content": 1})

	require.True(t, iter.Next())
	require.Equal(t, "testns:sta:4294967297:_field_lengths", string(iter.Key()))
	requireFieldLengths(t, iter.Value(), map[string]uint32{"id": 1, "content": 1})

	assert.False(t, iter.Next()) // End of data.

}

func TestDBFormatCompactDeletes(t *testing.T) {
	// This test ensures that deleted documents are actually removed from posting lists after
	// compacting deletes.
	db := mustOpenDB(t, testfs.MakeTempDir(t))

	doc1 := docWithIDAndText(t, 1, "one")

	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.AddDocument(doc1))
	require.NoError(t, w.Flush())

	w, err = NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.UpdateDocument(doc1.Field("id"), doc1))
	require.NoError(t, w.Flush())

	w, err = NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.CompactDeletes())
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
	assert.Equal(t, uint32(2), BytesToUint32(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:4294967297:_id", string(iter.Key())) // first doc id field content
	assert.Equal(t, uint64(1<<32|1), BytesToUint64(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:4294967297:id", string(iter.Key())) // first doc id field content
	assert.Equal(t, "1", string(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:doc:4294967297:text", string(iter.Key())) // first doc text field content
	assert.Equal(t, "one", string(iter.Value()))

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:1:id", string(iter.Key())) // ngram "1", field "id" posting list
	pl1ID, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{1<<32 | 1}, pl1ID.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:gra:one:text", string(iter.Key())) // ngram "one", field content PL
	plOneContent, err := posting.Unmarshal(iter.Value())
	require.NoError(t, err)
	assert.Equal(t, []uint64{1<<32 | 1}, plOneContent.ToArray())

	require.True(t, iter.Next())
	require.Equal(t, "testns:sta:4294967297:_field_lengths", string(iter.Key()))
	requireFieldLengths(t, iter.Value(), map[string]uint32{"id": 1, "text": 1})

	assert.False(t, iter.Next()) // End of data.
}

// TestCompactDeletesPreservesFrequencies checks that CompactDeletes removes
// deleted docs from a shared posting list while preserving the surviving docs'
// term frequencies (rather than collapsing them to 1). Three docs share the
// ngram "abc" and two are deleted, so the survivor is recovered from the middle
// of the counted list across multiple deletions.
func TestCompactDeletesPreservesFrequencies(t *testing.T) {
	db := mustOpenDB(t, testfs.MakeTempDir(t))
	docSchema := schema.NewDocumentSchema(
		[]types.FieldSchema{
			schema.MustFieldSchema(types.KeywordField, "id", true),
			schema.MustFieldSchema(types.SparseNgramField, "content", true),
		},
	)
	// All three docs contain ngram "abc" with distinct (>1) frequencies, so the
	// "abc" posting list is stored in counted form: {doc1: 3, doc2: 5, doc3: 7}.
	docs := []struct {
		id      string
		content string
		wantTF  uint32
	}{
		{"1", "abc abc abc", 3},
		{"2", "abc abc abc abc abc", 5},
		{"3", "abc abc abc abc abc abc abc", 7},
	}
	w, err := NewWriter(db, "testns")
	require.NoError(t, err)
	for _, d := range docs {
		require.NoError(t, w.AddDocument(docSchema.MustMakeDocument(map[string][]byte{
			"id":      []byte(d.id),
			"content": []byte(d.content),
		})))
	}
	require.NoError(t, w.Flush())

	abcKey := postingListKey("testns", "abc", "content")

	// Capture doc3's (the survivor's) docID and confirm pre-delete frequencies.
	doc3ID := func() uint64 {
		plBytes, closer, err := db.Get(abcKey)
		require.NoError(t, err)
		defer closer.Close()
		pl, err := posting.Unmarshal(plBytes)
		require.NoError(t, err)
		ids := pl.ToArray()
		require.Len(t, ids, 3)
		for i, d := range docs {
			assert.Equal(t, d.wantTF, pl.Frequency(ids[i]))
		}
		return ids[2]
	}()

	// Delete doc1 and doc2 (adds their docIDs to the deletes list; posting lists
	// still contain them until compaction).
	doc1 := docSchema.MustMakeDocument(map[string][]byte{"id": []byte("1")})
	doc2 := docSchema.MustMakeDocument(map[string][]byte{"id": []byte("2")})
	w, err = NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.DeleteDocumentByMatchField(doc1.Field("id")))
	require.NoError(t, w.DeleteDocumentByMatchField(doc2.Field("id")))
	require.NoError(t, w.Flush())

	// Compact: unmarshals the "abc" posting list into a MergeList and removes
	// both deleted docs in one RemoveAll pass, which must preserve doc3's TF.
	w, err = NewWriter(db, "testns")
	require.NoError(t, err)
	require.NoError(t, w.CompactDeletes())
	require.NoError(t, w.Flush())

	plBytes, closer, err := db.Get(abcKey)
	require.NoError(t, err)
	defer closer.Close()

	pl, err := posting.Unmarshal(plBytes)
	require.NoError(t, err)
	assert.Equal(t, []uint64{doc3ID}, pl.ToArray(), "doc1 and doc2 should be compacted out")
	assert.Equal(t, uint32(7), pl.Frequency(doc3ID), "survivor must keep its term frequency after compaction")

	// Also assert via the no-copy read path the query engine uses.
	roList, err := posting.UnmarshalReadOnly(plBytes)
	require.NoError(t, err)
	assert.Equal(t, uint32(7), roList.Frequency(doc3ID))
}

// WRITE:
// Test new index size is ~equal~ to old index size?
// Benchmark index performance?

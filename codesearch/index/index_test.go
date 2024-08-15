package index

import (
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func hash(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return h.Sum64()
}

func docWithName(name string) types.Document {
	return types.NewMapDocument(
		hash(name),
		map[string]types.NamedField{
			name: types.NewNamedField(types.TrigramField, name, []byte(name), true /*=stored*/),
		},
	)
}

func docWithID(id uint64) types.Document {
	return types.NewMapDocument(
		id,
		map[string]types.NamedField{
			"name": types.NewNamedField(types.TrigramField, "name", []byte(fmt.Sprintf("doc-%d", id)), true /*=stored*/),
		},
	)
}

func docWithIDAndText(id uint64, text string) types.Document {
	return types.NewMapDocument(
		id,
		map[string]types.NamedField{
			"text": types.NewNamedField(types.TrigramField, "text", []byte(text), true /*=stored*/),
		},
	)
}

func extractFieldMatches(docMatches []types.DocumentMatch) map[string][]uint64 {
	m := make(map[string][]uint64)
	for _, docMatch := range docMatches {
		for _, fieldName := range docMatch.FieldNames() {
			m[fieldName] = append(m[fieldName], docMatch.Docid())
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
	assert.Equal(t, map[string][]uint64{"name": {1, 2, 3}}, extractFieldMatches(matches))

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
	assert.Equal(t, map[string][]uint64{"name": {1, 3}}, extractFieldMatches(matches))
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
	assert.Equal(t, map[string][]uint64{"text": {1}}, extractFieldMatches(matches))

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
	assert.Equal(t, map[string][]uint64{"text": {5}}, extractFieldMatches(matches))

	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {2, 3, 4, 5}}, extractFieldMatches(matches))
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

	doc := types.NewMapDocument(
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

	doc := types.NewMapDocument(
		1,
		map[string]types.NamedField{
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
	assert.Equal(t, map[string][]uint64{"field_a": {1}}, extractFieldMatches(matches))

	// docs should be searchable by non-stored fields
	matches, err = r.RawQuery(`(:eq field_b unstored)`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"field_b": {1}}, extractFieldMatches(matches))

	// docs should be searchable by both stored and non-stored fields
	matches, err = r.RawQuery(`(:or (:eq field_b unstored) (:eq field_a stored))`)
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"field_b": {1}, "field_a": {1}}, extractFieldMatches(matches))

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
	assert.Equal(t, map[string][]uint64{"text": {1, 2, 3}}, extractFieldMatches(matches))

	matches, err = r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1}}, extractFieldMatches(matches))

	r = NewReader(ctx, db, "namespace-b")
	matches, err = r.RawQuery("(:all)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1, 2, 3, 4}}, extractFieldMatches(matches))

	matches, err = r.RawQuery("(:eq text pab)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {4}}, extractFieldMatches(matches))
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
	assert.Equal(t, map[string][]uint64{"text": {1, 2, 3}}, extractFieldMatches(matches))

	matches, err = r.RawQuery("(:none)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{}, extractFieldMatches(matches))

	matches, err = r.RawQuery("(:eq text one)")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1}}, extractFieldMatches(matches))

	matches, err = r.RawQuery("(:or (:eq text one) (:eq text bar))")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {1, 2}}, extractFieldMatches(matches))

	matches, err = r.RawQuery("(:eq text \" ba\")")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {2, 3}}, extractFieldMatches(matches))

	matches, err = r.RawQuery("(:and (:eq text \" ba\") (:eq text two))")
	require.NoError(t, err)
	assert.Equal(t, map[string][]uint64{"text": {2}}, extractFieldMatches(matches))

	_, err = r.RawQuery("(:and (:)") // invalid q
	require.Error(t, err)
}

// WRITE:
// Test new index size is ~equal~ to old index size?
// Benchmark index performance?

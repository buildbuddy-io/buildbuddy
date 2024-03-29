package index

import (
	"fmt"
	"hash/fnv"
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

	r := NewReader(db, "testing-namespace")
	docIDs, err := r.RawQuery([]byte("(:all)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, docIDs)

	// Delete a doc
	w, err = NewWriter(db, "testing-namespace")
	if err != nil {
		t.Fatal(err)
	}
	w.DeleteDocument(2)
	require.NoError(t, w.Flush())

	r = NewReader(db, "testing-namespace")
	docIDs, err = r.RawQuery([]byte("(:all)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 3}, docIDs)
}

func TestIncrementalIndexing(t *testing.T) {
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

	r := NewReader(db, "testing-namespace")
	docIDs, err := r.RawQuery([]byte("(:eq text one)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, docIDs)

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

	r = NewReader(db, "testing-namespace")
	docIDs, err = r.RawQuery([]byte("(:eq text one)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{5}, docIDs)

	docIDs, err = r.RawQuery([]byte("(:all)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{2, 3, 4, 5}, docIDs)
}

func TestUnkonwnTokenType(t *testing.T) {
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
	r := NewReader(db, "testing-namespace")
	docIDs, err := r.RawQuery([]byte(`(:eq field_a stored)`))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, docIDs)

	// docs should be searchable by non-stored fields
	docIDs, err = r.RawQuery([]byte(`(:eq field_b unstored)`))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, docIDs)

	// stored document should only contain stored fields
	rdoc, err := r.GetStoredDocument(1)
	require.NoError(t, err)
	require.Equal(t, []byte("stored"), rdoc.Field("field_a").Contents())
	require.Nil(t, rdoc.Field("field_b").Contents())
}

func TestNamespaceSeparation(t *testing.T) {
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

	r := NewReader(db, "namespace-a")
	docIDs, err := r.RawQuery([]byte("(:all)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, docIDs)

	docIDs, err = r.RawQuery([]byte("(:eq text one)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, docIDs)

	r = NewReader(db, "namespace-b")
	docIDs, err = r.RawQuery([]byte("(:all)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3, 4}, docIDs)

	docIDs, err = r.RawQuery([]byte("(:eq text pab)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{4}, docIDs)
}

func TestSQuery(t *testing.T) {
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

	r := NewReader(db, "testing-namespace")
	docIDs, err := r.RawQuery([]byte("(:all)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2, 3}, docIDs)

	docIDs, err = r.RawQuery([]byte("(:none)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{}, docIDs)

	docIDs, err = r.RawQuery([]byte("(:eq text one)"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1}, docIDs)

	docIDs, err = r.RawQuery([]byte("(:or (:eq text one) (:eq text bar))"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{1, 2}, docIDs)

	docIDs, err = r.RawQuery([]byte("(:eq text \" ba\")"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{2, 3}, docIDs)

	docIDs, err = r.RawQuery([]byte("(:and (:eq text \" ba\") (:eq text two))"))
	require.NoError(t, err)
	assert.Equal(t, []uint64{2}, docIDs)

	_, err = r.RawQuery([]byte("(:and (:)")) // invalid q
	require.Error(t, err)
}

// WRITE:
// Test new index size is ~equal~ to old index size?
// Benchmark index performance?

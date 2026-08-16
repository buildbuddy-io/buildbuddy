package watcher_test

import (
	"context"
	"errors"
	"iter"
	"sort"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/dns/watcher"
	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeObject is one object in the fake store: its bytes and its GCS generation.
type fakeObject struct {
	data       []byte
	generation int64
}

// fakeStore is an in-memory watcher.Store. Fields are mutated directly between
// Poll() calls (the watcher polls from a single goroutine, and tests drive
// Poll() synchronously, so no locking is needed). reads counts ReadBlob calls
// per object, so tests can assert unchanged objects aren't re-downloaded.
type fakeStore struct {
	objects  map[string]*fakeObject
	listErr  error
	readErrs map[string]error
	reads    map[string]int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		objects:  map[string]*fakeObject{},
		readErrs: map[string]error{},
		reads:    map[string]int{},
	}
}

// put writes (or overwrites) an object, bumping its generation the way GCS does
// on every write so the watcher notices the change.
func (s *fakeStore) put(name string, data []byte) {
	gen := int64(1)
	if cur, ok := s.objects[name]; ok {
		gen = cur.generation + 1
	}
	s.objects[name] = &fakeObject{data: data, generation: gen}
}

func (s *fakeStore) List(ctx context.Context, prefix string) iter.Seq2[gcs.ObjectAttrs, error] {
	return func(yield func(gcs.ObjectAttrs, error) bool) {
		if s.listErr != nil {
			yield(gcs.ObjectAttrs{}, s.listErr)
			return
		}
		names := make([]string, 0, len(s.objects))
		for name := range s.objects {
			if strings.HasPrefix(name, prefix) {
				names = append(names, name)
			}
		}
		sort.Strings(names)
		for _, name := range names {
			if !yield(gcs.ObjectAttrs{Name: name, Generation: s.objects[name].generation}, nil) {
				return
			}
		}
	}
}

func (s *fakeStore) ReadBlob(ctx context.Context, name string) ([]byte, error) {
	s.reads[name]++
	if err := s.readErrs[name]; err != nil {
		return nil, err
	}
	obj, ok := s.objects[name]
	if !ok {
		return nil, errors.New("object not found")
	}
	return obj.data, nil
}

// collector captures the updates a watcher emits.
type collector struct{ updates []watcher.Update }

func (c *collector) onUpdate(u watcher.Update) { c.updates = append(c.updates, u) }

func (c *collector) last() watcher.Update {
	return c.updates[len(c.updates)-1]
}

func newWatcher(store watcher.Store) (*collector, *watcher.Watcher) {
	c := &collector{}
	// interval is irrelevant: tests drive Poll() directly.
	return c, watcher.New(store, ".zone", 0, c.onUpdate)
}

func changedNames(u watcher.Update) []string {
	names := make([]string, 0, len(u.Changed))
	for name := range u.Changed {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func TestWatcher_DeliversMatchingObjects(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", []byte("a"))
	store.put("b.zone", []byte("b"))
	store.put("notes.txt", []byte("ignored")) // wrong suffix

	c, w := newWatcher(store)
	w.Poll(context.Background())

	require.Len(t, c.updates, 1)
	assert.Equal(t, []string{"a.zone", "b.zone"}, changedNames(c.last()))
	assert.Equal(t, []byte("a"), c.last().Changed["a.zone"])
	assert.Empty(t, c.last().Removed)
	assert.Zero(t, store.reads["notes.txt"], "non-matching object must not be read")
}

func TestWatcher_SkipsUnchangedObjects(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", []byte("a"))

	c, w := newWatcher(store)
	w.Poll(context.Background())
	require.Len(t, c.updates, 1)
	require.Equal(t, 1, store.reads["a.zone"])

	// Nothing changed: no re-read, no further update.
	w.Poll(context.Background())
	w.Poll(context.Background())
	assert.Equal(t, 1, store.reads["a.zone"], "unchanged object must not be re-downloaded")
	assert.Len(t, c.updates, 1, "unchanged poll must not deliver an update")
}

func TestWatcher_DeliversChanges(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", []byte("v1"))

	c, w := newWatcher(store)
	w.Poll(context.Background())

	store.put("a.zone", []byte("v2"))
	w.Poll(context.Background())

	require.Len(t, c.updates, 2)
	assert.Equal(t, []byte("v2"), c.last().Changed["a.zone"])
}

func TestWatcher_DeliversRemovals(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", []byte("a"))
	store.put("b.zone", []byte("b"))

	c, w := newWatcher(store)
	w.Poll(context.Background())

	delete(store.objects, "b.zone")
	w.Poll(context.Background())

	require.Len(t, c.updates, 2)
	assert.Empty(t, c.last().Changed)
	assert.Equal(t, []string{"b.zone"}, c.last().Removed)
}

func TestWatcher_RetriesAfterReadError(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", []byte("a"))
	store.readErrs["a.zone"] = errors.New("transient")

	c, w := newWatcher(store)
	w.Poll(context.Background())
	assert.Empty(t, c.updates, "a failed read delivers nothing")

	// Recover: the generation was never recorded, so the object isn't skipped.
	delete(store.readErrs, "a.zone")
	w.Poll(context.Background())
	require.Len(t, c.updates, 1)
	assert.Equal(t, []byte("a"), c.last().Changed["a.zone"])
}

func TestWatcher_ListErrorPreservesState(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", []byte("a"))

	c, w := newWatcher(store)
	w.Poll(context.Background())
	require.Len(t, c.updates, 1)

	// A failed listing abandons the poll without touching tracked state.
	store.listErr = errors.New("GCS unavailable")
	w.Poll(context.Background())
	assert.Len(t, c.updates, 1, "a failed listing must not deliver an update")

	// After recovery, nothing changed, so still no new update (state intact).
	store.listErr = nil
	w.Poll(context.Background())
	assert.Len(t, c.updates, 1)
}

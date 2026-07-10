package server

import (
	"context"
	"errors"
	"iter"
	"sort"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/miekg/dns"
)

// fakeObject is one object in the fake store: its bytes and its GCS generation.
type fakeObject struct {
	data       []byte
	generation int64
}

// fakeStore is an in-memory zoneStore for exercising the watcher. Objects and
// error injection are set directly on the fields between poll() calls (the
// watcher polls from a single goroutine, and tests drive poll() synchronously,
// so no locking is needed).
type fakeStore struct {
	objects  map[string]*fakeObject
	listErr  error
	readErrs map[string]error
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[string]*fakeObject{}, readErrs: map[string]error{}}
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
			obj := s.objects[name]
			if !yield(gcs.ObjectAttrs{Name: name, Generation: obj.generation}, nil) {
				return
			}
		}
	}
}

func (s *fakeStore) ReadBlob(ctx context.Context, name string) ([]byte, error) {
	if err := s.readErrs[name]; err != nil {
		return nil, err
	}
	obj, ok := s.objects[name]
	if !ok {
		return nil, errors.New("object not found")
	}
	return obj.data, nil
}

// zoneFileWithA returns the bytes of a minimal valid zone file for apex, whose
// apex A record points at addr (so different versions of the same file can be
// told apart).
func zoneFileWithA(apex, addr string) []byte {
	lines := []string{
		apex + " 60 IN SOA ns1.example. host.example. 1 21600 3600 259200 300",
		apex + " 60 IN A " + addr,
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

// servedApexes returns the sorted apexes of the zones the handler currently
// serves, reading straight from the swapped-in snapshot.
func servedApexes(h *Handler) []string {
	d := h.data.Load()
	apexes := make([]string, 0, len(d.zones))
	for _, z := range d.zones {
		apexes = append(apexes, z.apex)
	}
	sort.Strings(apexes)
	return apexes
}

// servedA returns the address of the (single) A record the handler serves for
// name, or "" if none.
func servedA(h *Handler, name string) string {
	d := h.data.Load()
	for _, rr := range d.records[dns.CanonicalName(name)] {
		if a, ok := rr.(*dns.A); ok {
			return a.A.String()
		}
	}
	return ""
}

func newWatcher(store zoneStore) (*Handler, *ZoneWatcher) {
	h := NewHandler(nil /*=env*/, nil /*=resources*/, nil /*=acme*/)
	// interval is irrelevant: tests drive poll() directly.
	return h, NewZoneWatcher(h, store, 0)
}

func TestZoneWatcher_LoadsValidZones(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", zoneFileWithA("a.example.", "1.1.1.1"))
	store.put("b.zone", zoneFileWithA("b.example.", "2.2.2.2"))
	// A non-".zone" object must be ignored entirely.
	store.put("notes.txt", []byte("not a zone file"))

	h, w := newWatcher(store)
	w.poll(context.Background())

	assert.Equal(t, []string{"a.example.", "b.example."}, servedApexes(h))
	assert.Equal(t, "1.1.1.1", servedA(h, "a.example."))
	assert.Equal(t, "2.2.2.2", servedA(h, "b.example."))
}

func TestZoneWatcher_BadFileDoesNotDisturbOthers(t *testing.T) {
	store := newFakeStore()
	store.put("good.zone", zoneFileWithA("good.example.", "1.1.1.1"))
	// Missing an SOA: rejected, but must not prevent good.zone from loading.
	store.put("nosoa.zone", []byte("nosoa.example. 60 IN A 3.3.3.3\n"))
	// Unparseable: also rejected independently.
	store.put("broken.zone", []byte("@@@ not a zone @@@\n"))

	h, w := newWatcher(store)
	w.poll(context.Background())

	assert.Equal(t, []string{"good.example."}, servedApexes(h))
}

func TestZoneWatcher_KeepsLastGoodWhenFileBecomesInvalid(t *testing.T) {
	store := newFakeStore()
	store.put("z.zone", zoneFileWithA("z.example.", "1.1.1.1"))

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	// The object is overwritten with an SOA-less (invalid) version: the watcher
	// must keep serving the last-good records rather than dropping the zone.
	store.put("z.zone", []byte("z.example. 60 IN A 9.9.9.9\n"))
	w.poll(context.Background())

	assert.Equal(t, []string{"z.example."}, servedApexes(h))
	assert.Equal(t, "1.1.1.1", servedA(h, "z.example."), "should still serve the last-good record")
}

func TestZoneWatcher_KeepsLastGoodOnReadError(t *testing.T) {
	store := newFakeStore()
	store.put("z.zone", zoneFileWithA("z.example.", "1.1.1.1"))

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	// A new version exists (so the generation changed and the watcher will try to
	// read it), but reads fail: keep last-good.
	store.put("z.zone", zoneFileWithA("z.example.", "9.9.9.9"))
	store.readErrs["z.zone"] = errors.New("transient read failure")
	w.poll(context.Background())

	assert.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	// Once reads recover, the new version is picked up (the failed read didn't
	// record the generation, so it isn't skipped as "already processed").
	delete(store.readErrs, "z.zone")
	w.poll(context.Background())
	assert.Equal(t, "9.9.9.9", servedA(h, "z.example."))
}

func TestZoneWatcher_PicksUpChanges(t *testing.T) {
	store := newFakeStore()
	store.put("z.zone", zoneFileWithA("z.example.", "1.1.1.1"))

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	store.put("z.zone", zoneFileWithA("z.example.", "5.5.5.5"))
	w.poll(context.Background())

	assert.Equal(t, "5.5.5.5", servedA(h, "z.example."))
}

// countingStore wraps fakeStore to count ReadBlob calls, so we can assert the
// watcher doesn't re-download objects whose generation is unchanged.
type countingStore struct {
	*fakeStore
	reads map[string]int
}

func (s *countingStore) ReadBlob(ctx context.Context, name string) ([]byte, error) {
	s.reads[name]++
	return s.fakeStore.ReadBlob(ctx, name)
}

func TestZoneWatcher_SkipsUnchangedObjects(t *testing.T) {
	base := newFakeStore()
	base.put("z.zone", zoneFileWithA("z.example.", "1.1.1.1"))
	store := &countingStore{fakeStore: base, reads: map[string]int{}}

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))
	require.Equal(t, 1, store.reads["z.zone"])

	// Nothing changed: the generation is unchanged, so no re-read should happen.
	w.poll(context.Background())
	w.poll(context.Background())
	assert.Equal(t, 1, store.reads["z.zone"], "unchanged object must not be re-downloaded")
}

// TestZoneWatcher_DoesNotReprocessUnchangedBadObject asserts the no-repeated-
// alert behavior indirectly: an unchanged bad object is not re-read (and thus
// not re-parsed or re-alerted) on subsequent polls.
func TestZoneWatcher_DoesNotReprocessUnchangedBadObject(t *testing.T) {
	base := newFakeStore()
	base.put("bad.zone", []byte("bad.example. 60 IN A 3.3.3.3\n")) // no SOA
	store := &countingStore{fakeStore: base, reads: map[string]int{}}

	_, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, 1, store.reads["bad.zone"])

	// The bad object is unchanged, so later polls skip it: no re-read, and hence
	// no repeated alert every interval.
	w.poll(context.Background())
	w.poll(context.Background())
	assert.Equal(t, 1, store.reads["bad.zone"], "unchanged bad object must not be re-read or re-alerted")
}

func TestZoneWatcher_DropsRemovedObject(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", zoneFileWithA("a.example.", "1.1.1.1"))
	store.put("b.zone", zoneFileWithA("b.example.", "2.2.2.2"))

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, []string{"a.example.", "b.example."}, servedApexes(h))

	delete(store.objects, "b.zone")
	w.poll(context.Background())

	assert.Equal(t, []string{"a.example."}, servedApexes(h))
}

func TestZoneWatcher_ListErrorKeepsCurrentZones(t *testing.T) {
	store := newFakeStore()
	store.put("a.zone", zoneFileWithA("a.example.", "1.1.1.1"))

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, []string{"a.example."}, servedApexes(h))

	store.listErr = errors.New("GCS unavailable")
	w.poll(context.Background())

	assert.Equal(t, []string{"a.example."}, servedApexes(h), "a failed listing must not drop the zones we already serve")
}

func TestZoneWatcher_EmptyBucketServesNothing(t *testing.T) {
	store := newFakeStore()

	h, w := newWatcher(store)
	w.poll(context.Background())

	assert.Empty(t, servedApexes(h))
}

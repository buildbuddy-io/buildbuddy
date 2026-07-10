package server

import (
	"context"
	"errors"
	"iter"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/miekg/dns"
)

// fakeStore is an in-memory zoneStore for exercising the watcher. Objects and
// error injection are set directly on the fields between poll() calls (the
// watcher polls from a single goroutine, and tests drive poll() synchronously,
// so no locking is needed).
type fakeStore struct {
	objects  map[string][]byte
	listErr  error
	readErrs map[string]error
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[string][]byte{}, readErrs: map[string]error{}}
}

func (s *fakeStore) List(ctx context.Context, prefix string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		if s.listErr != nil {
			yield("", s.listErr)
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
			if !yield(name, nil) {
				return
			}
		}
	}
}

func (s *fakeStore) ReadBlob(ctx context.Context, name string) ([]byte, error) {
	if err := s.readErrs[name]; err != nil {
		return nil, err
	}
	data, ok := s.objects[name]
	if !ok {
		return nil, errors.New("object not found")
	}
	return data, nil
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
	store.objects["a.zone"] = zoneFileWithA("a.example.", "1.1.1.1")
	store.objects["b.zone"] = zoneFileWithA("b.example.", "2.2.2.2")
	// A non-".zone" object must be ignored entirely.
	store.objects["notes.txt"] = []byte("not a zone file")

	h, w := newWatcher(store)
	w.poll(context.Background())

	assert.Equal(t, []string{"a.example.", "b.example."}, servedApexes(h))
	assert.Equal(t, "1.1.1.1", servedA(h, "a.example."))
	assert.Equal(t, "2.2.2.2", servedA(h, "b.example."))
}

func TestZoneWatcher_BadFileDoesNotDisturbOthers(t *testing.T) {
	store := newFakeStore()
	store.objects["good.zone"] = zoneFileWithA("good.example.", "1.1.1.1")
	// Missing an SOA: rejected, but must not prevent good.zone from loading.
	store.objects["nosoa.zone"] = []byte("nosoa.example. 60 IN A 3.3.3.3\n")
	// Unparseable: also rejected independently.
	store.objects["broken.zone"] = []byte("@@@ not a zone @@@\n")

	h, w := newWatcher(store)
	w.poll(context.Background())

	assert.Equal(t, []string{"good.example."}, servedApexes(h))
}

func TestZoneWatcher_KeepsLastGoodWhenFileBecomesInvalid(t *testing.T) {
	store := newFakeStore()
	store.objects["z.zone"] = zoneFileWithA("z.example.", "1.1.1.1")

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	// The object is overwritten with an SOA-less (invalid) version: the watcher
	// must keep serving the last-good records rather than dropping the zone.
	store.objects["z.zone"] = []byte("z.example. 60 IN A 9.9.9.9\n")
	w.poll(context.Background())

	assert.Equal(t, []string{"z.example."}, servedApexes(h))
	assert.Equal(t, "1.1.1.1", servedA(h, "z.example."), "should still serve the last-good record")
}

func TestZoneWatcher_KeepsLastGoodOnReadError(t *testing.T) {
	store := newFakeStore()
	store.objects["z.zone"] = zoneFileWithA("z.example.", "1.1.1.1")

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	// The object still exists in the listing but reads fail: keep last-good.
	store.readErrs["z.zone"] = errors.New("transient read failure")
	w.poll(context.Background())

	assert.Equal(t, "1.1.1.1", servedA(h, "z.example."))
}

func TestZoneWatcher_PicksUpChanges(t *testing.T) {
	store := newFakeStore()
	store.objects["z.zone"] = zoneFileWithA("z.example.", "1.1.1.1")

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	store.objects["z.zone"] = zoneFileWithA("z.example.", "5.5.5.5")
	w.poll(context.Background())

	assert.Equal(t, "5.5.5.5", servedA(h, "z.example."))
}

func TestZoneWatcher_DropsRemovedObject(t *testing.T) {
	store := newFakeStore()
	store.objects["a.zone"] = zoneFileWithA("a.example.", "1.1.1.1")
	store.objects["b.zone"] = zoneFileWithA("b.example.", "2.2.2.2")

	h, w := newWatcher(store)
	w.poll(context.Background())
	require.Equal(t, []string{"a.example.", "b.example."}, servedApexes(h))

	delete(store.objects, "b.zone")
	w.poll(context.Background())

	assert.Equal(t, []string{"a.example."}, servedApexes(h))
}

func TestZoneWatcher_ListErrorKeepsCurrentZones(t *testing.T) {
	store := newFakeStore()
	store.objects["a.zone"] = zoneFileWithA("a.example.", "1.1.1.1")

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

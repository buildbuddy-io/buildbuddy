package server

import (
	"sort"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/dns/watcher"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/miekg/dns"
)

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

func newUpdater() (*Handler, *zoneUpdater) {
	h := NewHandler(nil /*=env*/, nil /*=resources*/, nil /*=acme*/)
	return h, &zoneUpdater{handler: h, files: make(map[string][]dns.RR)}
}

func changedUpdate(objects map[string][]byte) watcher.Update {
	return watcher.Update{Changed: objects}
}

func TestZoneUpdater_LoadsValidZones(t *testing.T) {
	h, u := newUpdater()
	u.apply(changedUpdate(map[string][]byte{
		"a.zone": zoneFileWithA("a.example.", "1.1.1.1"),
		"b.zone": zoneFileWithA("b.example.", "2.2.2.2"),
	}))

	assert.Equal(t, []string{"a.example.", "b.example."}, servedApexes(h))
	assert.Equal(t, "1.1.1.1", servedA(h, "a.example."))
	assert.Equal(t, "2.2.2.2", servedA(h, "b.example."))
}

func TestZoneUpdater_RejectsBadFileKeepsOthers(t *testing.T) {
	h, u := newUpdater()
	u.apply(changedUpdate(map[string][]byte{
		"good.zone":   zoneFileWithA("good.example.", "1.1.1.1"),
		"nosoa.zone":  []byte("nosoa.example. 60 IN A 3.3.3.3\n"), // no SOA
		"broken.zone": []byte("@@@ not a zone @@@\n"),             // unparseable
	}))

	assert.Equal(t, []string{"good.example."}, servedApexes(h))
}

func TestZoneUpdater_KeepsLastGoodWhenFileBecomesInvalid(t *testing.T) {
	h, u := newUpdater()
	u.apply(changedUpdate(map[string][]byte{"z.zone": zoneFileWithA("z.example.", "1.1.1.1")}))
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	// A new (invalid) version of the same object: keep serving the last-good.
	u.apply(changedUpdate(map[string][]byte{"z.zone": []byte("z.example. 60 IN A 9.9.9.9\n")}))

	assert.Equal(t, []string{"z.example."}, servedApexes(h))
	assert.Equal(t, "1.1.1.1", servedA(h, "z.example."), "should still serve the last-good record")
}

func TestZoneUpdater_PicksUpChanges(t *testing.T) {
	h, u := newUpdater()
	u.apply(changedUpdate(map[string][]byte{"z.zone": zoneFileWithA("z.example.", "1.1.1.1")}))
	require.Equal(t, "1.1.1.1", servedA(h, "z.example."))

	u.apply(changedUpdate(map[string][]byte{"z.zone": zoneFileWithA("z.example.", "5.5.5.5")}))

	assert.Equal(t, "5.5.5.5", servedA(h, "z.example."))
}

func TestZoneUpdater_DropsRemovedObject(t *testing.T) {
	h, u := newUpdater()
	u.apply(changedUpdate(map[string][]byte{
		"a.zone": zoneFileWithA("a.example.", "1.1.1.1"),
		"b.zone": zoneFileWithA("b.example.", "2.2.2.2"),
	}))
	require.Equal(t, []string{"a.example.", "b.example."}, servedApexes(h))

	u.apply(watcher.Update{Removed: []string{"b.zone"}})

	assert.Equal(t, []string{"a.example."}, servedApexes(h))
}

func TestZoneUpdater_RemovingUnknownObjectIsNoop(t *testing.T) {
	h, u := newUpdater()
	u.apply(changedUpdate(map[string][]byte{"a.zone": zoneFileWithA("a.example.", "1.1.1.1")}))
	require.Equal(t, []string{"a.example."}, servedApexes(h))

	// Removing an object we never served (e.g. one that only ever failed to
	// parse) changes nothing.
	u.apply(watcher.Update{Removed: []string{"never-seen.zone"}})

	assert.Equal(t, []string{"a.example."}, servedApexes(h))
}

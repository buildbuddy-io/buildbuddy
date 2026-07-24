package server

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/dns/watcher"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/miekg/dns"
)

// zoneFileWithSerial returns the bytes of a minimal valid zone file for apex
// with the given SOA serial, whose apex A record points at addr (so different
// versions of the same file can be told apart).
func zoneFileWithSerial(apex, addr string, soaSerial uint32) []byte {
	lines := []string{
		fmt.Sprintf("%s 60 IN SOA ns1.example. host.example. %d 21600 3600 259200 300", apex, soaSerial),
		apex + " 60 IN A " + addr,
	}
	return []byte(strings.Join(lines, "\n") + "\n")
}

// zoneFileWithA is zoneFileWithSerial with a fixed serial of 1.
func zoneFileWithA(apex, addr string) []byte {
	return zoneFileWithSerial(apex, addr, 1)
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

func TestZoneUpdater_ExportsZoneSerials(t *testing.T) {
	_, u := newUpdater()
	u.apply(changedUpdate(map[string][]byte{
		"a.zone": zoneFileWithSerial("a.example.", "1.1.1.1", 7),
		"b.zone": zoneFileWithSerial("b.example.", "2.2.2.2", 42),
	}))

	serialOf := func(apex string) float64 {
		return testutil.ToFloat64(metrics.DNSServerZoneSerial.With(prometheus.Labels{metrics.DNSZoneLabel: apex}))
	}
	assert.Equal(t, 7.0, serialOf("a.example."))
	assert.Equal(t, 42.0, serialOf("b.example."))

	// A new version of a zone updates its serial.
	u.apply(changedUpdate(map[string][]byte{"a.zone": zoneFileWithSerial("a.example.", "1.1.1.1", 8)}))
	assert.Equal(t, 8.0, serialOf("a.example."))

	// A removed zone drops off the metric entirely.
	u.apply(watcher.Update{Removed: []string{"b.zone"}})
	assert.Equal(t, 1, testutil.CollectAndCount(metrics.DNSServerZoneSerial))
	assert.Equal(t, 8.0, serialOf("a.example."))
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

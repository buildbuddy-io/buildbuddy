// Package testpprof provides test helpers for inspecting pprof labels.
package testpprof

import (
	"bytes"
	"maps"
	"regexp"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// WaitTimeout is only reached if a condition that is guaranteed to become
	// true never does, or if the machine is stalled.
	WaitTimeout  = 60 * time.Second
	pollInterval = 10 * time.Millisecond
)

// headerRE matches the first line of a goroutine in a debug=2 goroutine dump,
// e.g. `goroutine 18 [chan receive, 2 minutes] {shared_subsystem: foo, k: "a b"}:`.
var headerRE = regexp.MustCompile(`^goroutine (\d+) \[[^\]]*\](?: \{(.*)\})?:$`)

type goroutine struct {
	id     int64
	labels map[string]string
	// frames is the text of the goroutine's function frames (not the file
	// lines, nor the "created by" line).
	frames string
}

// dump returns all current goroutines. The debug=2 format identifies each
// goroutine, unlike the grouped debug=1 format.
func dump(t testing.TB) []goroutine {
	var buf bytes.Buffer
	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&buf, 2))
	var out []goroutine
	for block := range strings.SplitSeq(buf.String(), "\n\n") {
		header, body, _ := strings.Cut(strings.TrimSpace(block), "\n")
		m := headerRE.FindStringSubmatch(header)
		if m == nil {
			continue
		}
		id, err := strconv.ParseInt(m[1], 10, 64)
		require.NoError(t, err)
		var frames []string
		for line := range strings.SplitSeq(body, "\n") {
			if strings.HasPrefix(line, "\t") || strings.HasPrefix(line, "created by ") || strings.HasPrefix(line, "[originating from ") {
				continue
			}
			frames = append(frames, line)
		}
		out = append(out, goroutine{id: id, labels: parseLabels(t, m[2]), frames: strings.Join(frames, "\n")})
	}
	return out
}

// parseLabels parses the runtime's `k: v, k2: "quoted v"` label list.
func parseLabels(t testing.TB, s string) map[string]string {
	labels := map[string]string{}
	token := func() string {
		if strings.HasPrefix(s, `"`) {
			q, err := strconv.QuotedPrefix(s)
			require.NoError(t, err, "parsing labels %q", s)
			s = s[len(q):]
			v, err := strconv.Unquote(q)
			require.NoError(t, err)
			return v
		}
		i := strings.IndexAny(s, ":,")
		if i < 0 {
			i = len(s)
		}
		v := s[:i]
		s = s[i:]
		return v
	}
	for s != "" {
		k := token()
		s = strings.TrimPrefix(s, ": ")
		v := token()
		s = strings.TrimPrefix(s, ", ")
		labels[k] = v
	}
	return labels
}

// Snapshot records what each goroutine's stack looked like at some point. Its
// methods only consider goroutines that have started running a function since
// then, so that a test can ignore goroutines left over from other tests in the
// same process. Goroutine IDs are never reused, but pooled workers (e.g.
// gRPC's) are, so a goroutine existing in the snapshot isn't enough to
// exclude it: it is excluded only if its stack already contained the function.
type Snapshot map[int64]string

// TakeSnapshot returns the current state of all goroutines.
func TakeSnapshot(t testing.TB) Snapshot {
	s := Snapshot{}
	for _, g := range dump(t) {
		s[g.id] = g.frames
	}
	return s
}

// isNew returns whether g runs funcSubstring and didn't at the time of s.
// funcSubstring may be several substrings joined by "&", all of which must
// appear in the stack (e.g. to tell a client goroutine from a server one that
// runs the same function).
func (s Snapshot) isNew(g goroutine, funcSubstring string) bool {
	return hasAll(g.frames, funcSubstring) && !hasAll(s[g.id], funcSubstring)
}

func hasAll(frames, funcSubstring string) bool {
	for sub := range strings.SplitSeq(funcSubstring, "&") {
		if !strings.Contains(frames, sub) {
			return false
		}
	}
	return true
}

// GoroutineLabels returns the label sets of all goroutines whose stack
// contains the function name substring funcSubstring. Goroutines without
// labels yield an empty map. Inlined functions are included in the stacks.
func GoroutineLabels(t testing.TB, funcSubstring string) []map[string]string {
	return Snapshot(nil).GoroutineLabels(t, funcSubstring)
}

// GoroutineLabels is like the package-level GoroutineLabels, but only for
// goroutines that started running funcSubstring after s was taken.
func (s Snapshot) GoroutineLabels(t testing.TB, funcSubstring string) []map[string]string {
	var out []map[string]string
	for _, g := range dump(t) {
		if s.isNew(g, funcSubstring) {
			out = append(out, g.labels)
		}
	}
	return out
}

// RequireGoroutineLabels waits until at least one goroutine that started
// running funcSubstring after s was taken exists, and every such goroutine has
// exactly the labels in want.
//
// This is a poll, so the caller must ensure that such a goroutine is
// guaranteed to exist until the test ends (e.g. it blocks until the test
// shuts something down).
func (s Snapshot) RequireGoroutineLabels(t testing.TB, funcSubstring string, want map[string]string) {
	t.Helper()
	var got []map[string]string
	deadline := time.Now().Add(WaitTimeout)
	for {
		got = s.GoroutineLabels(t, funcSubstring)
		if len(got) > 0 && allEqual(got, want) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("want all goroutines newly matching %q to have labels %v, last saw %v", funcSubstring, want, got)
		}
		time.Sleep(pollInterval)
	}
}

func allEqual(sets []map[string]string, want map[string]string) bool {
	for _, l := range sets {
		if !maps.Equal(l, want) {
			return false
		}
	}
	return true
}

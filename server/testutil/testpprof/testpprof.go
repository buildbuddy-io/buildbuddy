// Package testpprof provides test helpers for inspecting pprof labels.
package testpprof

import (
	"bytes"
	"fmt"
	"maps"
	"regexp"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	waitTimeout  = 10 * time.Second
	pollInterval = 10 * time.Millisecond
)

var (
	labelsRE    = regexp.MustCompile(`labels: (\{.*\})`)
	labelPairRE = regexp.MustCompile(`"([^"]*)":\s*"((?:[^"\\]|\\.)*)"`)
)

// GoroutineLabels returns the label sets of all goroutines whose stack contains
// funcSubstring. Goroutines without labels yield an empty map.
func GoroutineLabels(t testing.TB, funcSubstring string) []map[string]string {
	var buf bytes.Buffer
	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&buf, 1))
	// With debug=1, goroutines with identical stacks and labels are grouped:
	// "N @ 0x... \n# labels: {...}\n#\t0x... func+0x..\n"
	var out []map[string]string
	for group := range strings.SplitSeq(buf.String(), "\n\n") {
		if !strings.Contains(group, funcSubstring) {
			continue
		}
		labels := map[string]string{}
		if m := labelsRE.FindStringSubmatch(group); m != nil {
			for _, p := range labelPairRE.FindAllStringSubmatch(m[1], -1) {
				labels[p[1]] = p[2]
			}
		}
		out = append(out, labels)
	}
	return out
}

// poll calls cond until it returns true, failing the test with msg() if it
// doesn't within waitTimeout.
func poll(t testing.TB, cond func() bool, msg func() string) {
	t.Helper()
	deadline := time.Now().Add(waitTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(msg())
		}
		time.Sleep(pollInterval)
	}
}

// RequireGoroutineLabels waits until at least one goroutine whose stack
// contains funcSubstring exists and every such goroutine has exactly the
// labels in want.
func RequireGoroutineLabels(t testing.TB, funcSubstring string, want map[string]string) {
	t.Helper()
	var got []map[string]string
	poll(t, func() bool {
		got = GoroutineLabels(t, funcSubstring)
		if len(got) == 0 {
			return false
		}
		for _, l := range got {
			if !maps.Equal(l, want) {
				return false
			}
		}
		return true
	}, func() string {
		return fmt.Sprintf("want all goroutines matching %q to have labels %v, last saw %v", funcSubstring, want, got)
	})
}

// RequireAnyGoroutineLabels waits until at least one goroutine whose stack
// contains funcSubstring has exactly the labels in want. Unlike
// RequireGoroutineLabels it tolerates other matching goroutines (e.g.
// leftovers from earlier tests in the same process).
func RequireAnyGoroutineLabels(t testing.TB, funcSubstring string, want map[string]string) {
	t.Helper()
	var got []map[string]string
	poll(t, func() bool {
		got = GoroutineLabels(t, funcSubstring)
		for _, l := range got {
			if maps.Equal(l, want) {
				return true
			}
		}
		return false
	}, func() string {
		return fmt.Sprintf("want a goroutine matching %q with labels %v, last saw %v", funcSubstring, want, got)
	})
}

// Package pprofutiltest provides test helpers for inspecting pprof labels.
package pprofutiltest

import (
	"bytes"
	"regexp"
	"runtime/pprof"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
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

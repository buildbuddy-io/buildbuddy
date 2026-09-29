package testserver

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const raceReport = `==================
WARNING: DATA RACE
Write at 0x00c000123456 by goroutine 7:
  main.main.func1()
      main.go:10 +0x44

Previous read at 0x00c000123456 by goroutine 6:
  main.main()
      main.go:12 +0x88
==================
`

func TestRaceDetector(t *testing.T) {
	for _, tc := range []struct {
		name   string
		output string
		// Size of each write, to check reports split across writes. 0 means
		// a single write.
		chunkSize   int
		wantReports int
	}{
		{name: "no races", output: "starting\nready\n", wantReports: 0},
		{name: "one race", output: "starting\n" + raceReport + "ready\n", wantReports: 1},
		{name: "one race in small writes", output: "starting\n" + raceReport + "ready\n", chunkSize: 3, wantReports: 1},
		{name: "two races", output: raceReport + "log line\n" + raceReport, chunkSize: 16, wantReports: 2},
		{name: "cut off race", output: "starting\n" + raceReport[:len(raceReport)/2], wantReports: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := &raceDetector{}
			var out bytes.Buffer
			w := d.Writer(&out)
			chunkSize := tc.chunkSize
			if chunkSize == 0 {
				chunkSize = len(tc.output)
			}
			for s := tc.output; len(s) > 0; {
				n := min(chunkSize, len(s))
				_, err := w.Write([]byte(s[:n]))
				require.NoError(t, err)
				s = s[n:]
			}
			w.Flush()

			// Output is passed through unchanged.
			require.Equal(t, tc.output, out.String())
			reports := d.Reports()
			require.Len(t, reports, tc.wantReports)
			for _, r := range reports {
				require.True(t, strings.HasPrefix(r, "WARNING: DATA RACE"), "report: %q", r)
			}
			if tc.name == "one race" {
				require.Contains(t, reports[0], "Previous read at 0x00c000123456 by goroutine 6:")
				require.NotContains(t, reports[0], "==================")
			}
		})
	}
}

func TestRaceDetector_SeparateStreams(t *testing.T) {
	d := &raceDetector{}
	stdout := d.Writer(&bytes.Buffer{})
	stderr := d.Writer(&bytes.Buffer{})
	// Interleaved writes to the two streams must not corrupt each other's
	// reports.
	half := len(raceReport) / 2
	_, _ = stderr.Write([]byte(raceReport[:half]))
	_, _ = stdout.Write([]byte("unrelated log line\n==================\n"))
	_, _ = stderr.Write([]byte(raceReport[half:]))
	stdout.Flush()
	stderr.Flush()
	reports := d.Reports()
	require.Len(t, reports, 1)
	require.Contains(t, reports[0], "Previous read at")
}

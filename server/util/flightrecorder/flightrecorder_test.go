package flightrecorder

import (
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime/metrics"
	"runtime/trace"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var testBuckets = []float64{0, 0.05, 0.1, 0.2, math.Inf(1)}

func hist(counts ...uint64) *metrics.Float64Histogram {
	return &metrics.Float64Histogram{Buckets: testBuckets, Counts: counts}
}

func TestCountAtOrAbove(t *testing.T) {
	h := &metrics.Float64Histogram{
		Buckets: []float64{math.Inf(-1), 0, 0.05, 0.1, 0.2, math.Inf(1)},
		Counts:  []uint64{0, 10, 5, 2, 1},
	}

	count, longest := countAtOrAbove(h, 0.1)
	require.Equal(t, uint64(3), count)
	require.Equal(t, 0.2, longest)

	// A threshold between boundaries rounds up to the next bucket.
	count, longest = countAtOrAbove(h, 0.07)
	require.Equal(t, uint64(3), count)
	require.Equal(t, 0.2, longest)

	count, longest = countAtOrAbove(h, 0)
	require.Equal(t, uint64(18), count)
	require.Equal(t, 0.2, longest)

	count, longest = countAtOrAbove(h, 1)
	require.Equal(t, uint64(0), count)
	require.Equal(t, 0.0, longest)
}

type dumpCall struct {
	label string
	pause time.Duration
}

// fakeWatcher returns a watcher whose gc histogram is served from *gc and
// whose dumps are appended to *dumps.
func fakeWatcher(t *testing.T, gc *[]uint64, dumps *[]dumpCall) *watcher {
	w := newWatcher(nil, 100*time.Millisecond, t.TempDir(), 5, time.Minute)
	w.read = func() []*metrics.Float64Histogram {
		return []*metrics.Float64Histogram{hist((*gc)...), hist(0, 0, 0, 0)}
	}
	w.dump = func(label string, pause time.Duration) { *dumps = append(*dumps, dumpCall{label, pause}) }
	return w
}

func TestPollDumpsOnlyWhenLongPausesIncrease(t *testing.T) {
	var dumps []dumpCall
	gc := []uint64{10, 5, 3, 0}
	w := fakeWatcher(t, &gc, &dumps)

	// The first poll records the baseline without dumping, even though the
	// histogram already holds long pauses.
	w.poll()
	require.Empty(t, dumps)

	// No change: no dump.
	w.poll()
	require.Empty(t, dumps)

	// Only short pauses were added: no dump.
	gc = []uint64{20, 9, 3, 0}
	w.poll()
	require.Empty(t, dumps)

	// A new pause in the 200ms+ bucket: dump, reporting that bucket.
	gc = []uint64{20, 9, 3, 1}
	w.poll()
	require.Equal(t, []dumpCall{{"gc", 200 * time.Millisecond}}, dumps)

	// A new pause in the 100ms bucket: dump again; the longest non-empty
	// bucket at or above the threshold is still 200ms.
	gc = []uint64{20, 9, 4, 1}
	w.poll()
	require.Equal(t, []dumpCall{{"gc", 200 * time.Millisecond}, {"gc", 200 * time.Millisecond}}, dumps)
}

func TestEffectiveThresholdRoundsUpToBucket(t *testing.T) {
	var dumps []dumpCall
	gc := []uint64{0, 0, 0, 0}
	w := fakeWatcher(t, &gc, &dumps)
	w.threshold = 0.07
	require.Equal(t, 100*time.Millisecond, w.effectiveThreshold())
}

func TestWriteDumpRateLimitsAndCaps(t *testing.T) {
	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{MinAge: time.Second})
	require.NoError(t, fr.Start())
	defer fr.Stop()

	dir := t.TempDir()
	w := newWatcher(fr, 100*time.Millisecond, dir, 2, time.Minute)
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	w.now = func() time.Time { return now }

	w.writeDump("gc", 150*time.Millisecond)
	files, err := filepath.Glob(filepath.Join(dir, "flightrecorder-20261029T120000Z-gc-150ms.trace"))
	require.NoError(t, err)
	require.Len(t, files, 1)
	st, err := os.Stat(files[0])
	require.NoError(t, err)
	require.Greater(t, st.Size(), int64(0))

	// Within min_dump_interval: skipped.
	now = now.Add(10 * time.Second)
	w.writeDump("gc", 300*time.Millisecond)
	files, _ = filepath.Glob(filepath.Join(dir, "*.trace"))
	require.Len(t, files, 1)

	// After the interval: written (dump 2 of 2).
	now = now.Add(time.Minute)
	w.writeDump("other", 300*time.Millisecond)
	files, _ = filepath.Glob(filepath.Join(dir, "*.trace"))
	require.Len(t, files, 2)

	// Cap reached: no more files.
	now = now.Add(time.Minute)
	w.writeDump("gc", 500*time.Millisecond)
	files, _ = filepath.Glob(filepath.Join(dir, "*.trace"))
	require.Len(t, files, 2)
}

func TestHandler(t *testing.T) {
	// Not running: 503.
	setGlobal(nil)
	rr := httptest.NewRecorder()
	Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/debug/flightrecorder", nil))
	require.Equal(t, http.StatusServiceUnavailable, rr.Code)

	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{MinAge: time.Second})
	require.NoError(t, fr.Start())
	defer fr.Stop()
	setGlobal(newWatcher(fr, 100*time.Millisecond, t.TempDir(), 5, time.Minute))
	defer setGlobal(nil)

	rr = httptest.NewRecorder()
	Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/debug/flightrecorder", nil))
	require.Equal(t, http.StatusOK, rr.Code)
	require.Equal(t, "application/octet-stream", rr.Header().Get("Content-Type"))
	require.Greater(t, rr.Body.Len(), 0)
}

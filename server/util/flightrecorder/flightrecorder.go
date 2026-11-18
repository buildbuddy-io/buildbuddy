// Package flightrecorder keeps a rolling window of the Go execution trace in
// memory and writes it to disk whenever the runtime records a stop-the-world
// pause longer than a configured threshold.
//
// Long pauses are usually caused by one goroutine that cannot be preempted
// (for example a large allocation that has to sweep the heap first). Nothing
// in the runtime metrics says which goroutine that was, but the execution
// trace does: the last P to stop and the stack it was running are both in the
// trace. Because such pauses are rare, tracing on demand never catches one;
// the flight recorder keeps the last few seconds continuously so the trace is
// already there when the pause is observed.
//
// The current window can also be fetched on demand from the monitoring port:
//
//	kubectl get --raw /api/v1/namespaces/<ns>/pods/<pod>:9090/proxy/debug/flightrecorder > trace.bin
//	go tool trace trace.bin
package flightrecorder

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"runtime/metrics"
	"runtime/trace"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	enabled         = flag.Bool("monitoring.flight_recorder.enabled", false, "If true, keep a rolling window of the Go execution trace in memory and dump it to a file whenever a stop-the-world pause exceeds monitoring.flight_recorder.pause_threshold. Costs roughly 1-2% CPU plus the window's memory.")
	pauseThreshold  = flag.Duration("monitoring.flight_recorder.pause_threshold", 100*time.Millisecond, "Dump the trace window when a stop-the-world pause at least this long is recorded. Rounded up to the nearest runtime histogram bucket.")
	minAge          = flag.Duration("monitoring.flight_recorder.min_age", 5*time.Second, "How much trace history to keep in memory, as a lower bound on the window's age. Busy servers produce tens of MB of trace per second, so max_bytes usually decides the window.")
	maxBytes        = flag.Int("monitoring.flight_recorder.max_bytes", 256<<20, "Upper bound on the in-memory trace window, in bytes. Takes precedence over min_age. The runtime treats this as a hint, not a hard limit.")
	pollInterval    = flag.Duration("monitoring.flight_recorder.poll_interval", 250*time.Millisecond, "How often to check the runtime pause histograms.")
	outputDir       = flag.String("monitoring.flight_recorder.output_dir", "", "Directory for trace dumps. Defaults to the OS temp dir.")
	maxDumps        = flag.Int("monitoring.flight_recorder.max_dumps", 5, "Stop writing dumps after this many. Recording continues so /debug/flightrecorder still works.")
	minDumpInterval = flag.Duration("monitoring.flight_recorder.min_dump_interval", time.Minute, "Minimum time between two automatic dumps.")
)

// watched lists the runtime histograms that trigger a dump. The "total"
// histograms cover the whole pause (stopping plus stopped plus restart), so
// they also catch every long "stopping" pause.
var watched = []struct{ metric, label string }{
	{"/sched/pauses/total/gc:seconds", "gc"},
	{"/sched/pauses/total/other:seconds", "other"},
}

// Enabled reports whether the flight recorder flag is on.
func Enabled() bool { return *enabled }

var (
	startOnce sync.Once
	startErr  error
	startStop func()

	globalMu sync.Mutex
	global   *watcher
)

// Start starts the flight recorder and the pause watcher. It returns a stop
// function that ends recording; callers should run it at shutdown. Start is
// idempotent: later calls return the same error and a no-op stop. It is a
// no-op when the feature is disabled.
func Start() (stop func(), err error) {
	if !*enabled {
		return func() {}, nil
	}
	startOnce.Do(func() {
		startStop = func() {}
		dir := *outputDir
		if dir == "" {
			dir = os.TempDir()
		}
		fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{
			MinAge:   *minAge,
			MaxBytes: uint64(*maxBytes),
		})
		if err := fr.Start(); err != nil {
			startErr = status.InternalErrorf("start flight recorder: %s", err)
			return
		}
		w := newWatcher(fr, *pauseThreshold, dir, *maxDumps, *minDumpInterval)
		ctx, cancel := context.WithCancel(context.Background())
		setGlobal(w)
		go w.run(ctx, *pollInterval)
		var stopOnce sync.Once
		startStop = func() {
			stopOnce.Do(func() {
				cancel()
				fr.Stop()
			})
		}
		log.Infof("Flight recorder enabled: dumping the last %s of execution trace to %s when a stop-the-world pause reaches %s", *minAge, dir, w.effectiveThreshold())
	})
	if startErr != nil {
		return func() {}, startErr
	}
	return startStop, nil
}

func setGlobal(w *watcher) {
	globalMu.Lock()
	defer globalMu.Unlock()
	global = w
}

func getGlobal() *watcher {
	globalMu.Lock()
	defer globalMu.Unlock()
	return global
}

// Handler returns an HTTP handler that writes the current trace window. It is
// meant for the monitoring mux at /debug/flightrecorder.
func Handler() http.Handler {
	return http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w := getGlobal()
		if w == nil || !w.fr.Enabled() {
			http.Error(rw, "flight recorder is not running", http.StatusServiceUnavailable)
			return
		}
		rw.Header().Set("Content-Type", "application/octet-stream")
		rw.Header().Set("Content-Disposition", `attachment; filename="flightrecorder.trace"`)
		if _, err := w.fr.WriteTo(rw); err != nil {
			log.Warningf("Flight recorder: write snapshot to HTTP response: %s", err)
		}
	})
}

type watcher struct {
	fr        *trace.FlightRecorder
	threshold float64 // seconds
	samples   []metrics.Sample
	last      []uint64 // per watched metric: count of pauses at or above threshold
	primed    bool

	dir             string
	maxDumps        int
	minDumpInterval time.Duration

	// Overridable for tests.
	read func() []*metrics.Float64Histogram // one entry per watched metric, nil if unavailable
	dump func(label string, pause time.Duration)
	now  func() time.Time

	mu        sync.Mutex
	dumps     int
	capLogged bool
	lastDump  time.Time
}

func newWatcher(fr *trace.FlightRecorder, threshold time.Duration, dir string, maxDumps int, minDumpInterval time.Duration) *watcher {
	w := &watcher{
		fr:              fr,
		threshold:       threshold.Seconds(),
		dir:             dir,
		maxDumps:        maxDumps,
		minDumpInterval: minDumpInterval,
		now:             time.Now,
	}
	w.read = w.readRuntime
	w.dump = w.writeDump
	for _, m := range watched {
		w.samples = append(w.samples, metrics.Sample{Name: m.metric})
	}
	w.last = make([]uint64, len(watched))
	return w
}

// readRuntime reads the watched histograms from runtime/metrics.
func (w *watcher) readRuntime() []*metrics.Float64Histogram {
	metrics.Read(w.samples)
	out := make([]*metrics.Float64Histogram, len(w.samples))
	for i, s := range w.samples {
		if s.Value.Kind() == metrics.KindFloat64Histogram {
			out[i] = s.Value.Float64Histogram()
		}
	}
	return out
}

func (w *watcher) run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.poll()
		}
	}
}

// poll reads the pause histograms and dumps the trace when the number of
// pauses at or above the threshold has grown since the last poll. The first
// poll only records the baseline so pauses from before startup do not fire.
func (w *watcher) poll() {
	for i, h := range w.read() {
		if h == nil {
			continue
		}
		count, longest := countAtOrAbove(h, w.threshold)
		if w.primed && count > w.last[i] {
			w.dump(watched[i].label, time.Duration(longest*float64(time.Second)))
		}
		w.last[i] = count
	}
	w.primed = true
}

// countAtOrAbove returns how many samples fall in buckets whose lower bound is
// at or above threshold, and the lower bound of the highest non-empty such
// bucket (0 if none).
func countAtOrAbove(h *metrics.Float64Histogram, threshold float64) (count uint64, longest float64) {
	for i, c := range h.Counts {
		lo := h.Buckets[i]
		if lo < threshold || math.IsInf(lo, 0) {
			continue
		}
		count += c
		if c > 0 {
			longest = lo
		}
	}
	return count, longest
}

// effectiveThreshold is the configured threshold rounded up to the next
// runtime histogram bucket boundary, which is what poll actually tests.
func (w *watcher) effectiveThreshold() time.Duration {
	configured := time.Duration(w.threshold * float64(time.Second))
	hs := w.read()
	if len(hs) == 0 || hs[0] == nil {
		return configured
	}
	for _, lo := range hs[0].Buckets {
		if lo >= w.threshold && !math.IsInf(lo, 0) {
			return time.Duration(lo * float64(time.Second))
		}
	}
	return configured
}

func (w *watcher) writeDump(label string, pause time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	if w.dumps >= w.maxDumps {
		if !w.capLogged {
			log.Infof("Flight recorder: observed a %s stop-the-world pause (%s) but max_dumps (%d) was reached; fetch /debug/flightrecorder manually", label, pause, w.maxDumps)
			w.capLogged = true
		}
		return
	}
	if !w.lastDump.IsZero() && now.Sub(w.lastDump) < w.minDumpInterval {
		log.Infof("Flight recorder: observed a %s stop-the-world pause (%s) within min_dump_interval of the previous dump; skipping", label, pause)
		return
	}
	path := filepath.Join(w.dir, fmt.Sprintf("flightrecorder-%s-%s-%dms.trace", now.UTC().Format("20060102T150405Z"), label, pause.Milliseconds()))
	f, err := os.Create(path)
	if err != nil {
		log.Warningf("Flight recorder: create %s: %s", path, err)
		return
	}
	n, werr := w.fr.WriteTo(f)
	cerr := f.Close()
	if werr != nil || cerr != nil {
		log.Warningf("Flight recorder: write %s: write err %v, close err %v", path, werr, cerr)
		return
	}
	w.dumps++
	w.lastDump = now
	log.Infof("Flight recorder: a %s stop-the-world pause of at least %s was recorded; wrote %d bytes of execution trace to %s (dump %d of %d)", label, pause, n, path, w.dumps, w.maxDumps)
}

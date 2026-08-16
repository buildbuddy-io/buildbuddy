// Generates a Grafana dashboard for Go runtime metrics (GC, allocation,
// scheduler, goroutines, memory, and contention).
//
// Requires the richer runtime/metrics set, which is registered via
// collectors.WithGoCollectorRuntimeMetrics in server/util/monitoring.
package main

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash"
	"github.com/grafana/grafana-foundation-sdk/go/dashboard"
	"github.com/grafana/grafana-foundation-sdk/go/heatmap"
	"github.com/grafana/grafana-foundation-sdk/go/timeseries"
)

// Label filter applied to every query. Uses the template variables from build().
const f = `region=~"$region",job=~"$job",pod_name=~"$pod"`

// UnitCores labels rate(...cpu_seconds_total) series, which are in CPU-cores.
const unitCores = dash.UnitShort

// --- CPU ---

func gcCPUFractionPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("GC CPU fraction", dash.UnitPercent).
		Description("Per-pod GC CPU as a fraction of that pod's total CPU capacity. This is the headline 'is GC expensive' signal; STW pauses alone badly understate it.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`100 * sum by (pod_name) (rate(go_cpu_classes_gc_total_cpu_seconds_total{%s}[$__rate_interval])) / sum by (pod_name) (rate(go_cpu_classes_total_cpu_seconds_total{%s}[$__rate_interval]))`, f, f),
			"{{pod_name}}",
		)).
		Thresholds(dashboard.NewThresholdsConfigBuilder().
			Mode(dashboard.ThresholdsModeAbsolute).
			Steps([]dashboard.Threshold{
				{Value: nil, Color: "green"},
				{Value: new(10.0), Color: "orange"},
				{Value: new(25.0), Color: "red"},
			}))
}

func gcCPUByClassPanel() *timeseries.PanelBuilder {
	p := dash.StackedTimeseries("GC CPU by class (cores)", unitCores).
		Description("GC CPU for this pod broken down by class (CPU-cores). 'mark assist' is the bad one: it means mutator goroutines are forced to help GC because allocation is outpacing it.")
	for _, c := range []struct{ metric, legend string }{
		{"go_cpu_classes_gc_mark_assist_cpu_seconds_total", "mark assist"},
		{"go_cpu_classes_gc_mark_dedicated_cpu_seconds_total", "mark dedicated"},
		{"go_cpu_classes_gc_mark_idle_cpu_seconds_total", "mark idle"},
		{"go_cpu_classes_gc_pause_cpu_seconds_total", "pause (STW)"},
	} {
		p = p.WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum(rate(%s{%s}[$__rate_interval]))`, c.metric, f), c.legend))
	}
	return p
}

func cpuByClassPanel() *timeseries.PanelBuilder {
	p := dash.StackedTimeseries("Total CPU by class (cores)", unitCores).
		Description("Where the runtime spends CPU for this pod (CPU-cores).")
	for _, c := range []struct{ metric, legend string }{
		{"go_cpu_classes_user_cpu_seconds_total", "user"},
		{"go_cpu_classes_gc_total_cpu_seconds_total", "gc"},
		{"go_cpu_classes_scavenge_total_cpu_seconds_total", "scavenge"},
		{"go_cpu_classes_idle_cpu_seconds_total", "idle"},
	} {
		p = p.WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum(rate(%s{%s}[$__rate_interval]))`, c.metric, f), c.legend))
	}
	return p
}

// --- Garbage collection ---

func gcPauseQuantilesPanel() *timeseries.PanelBuilder {
	p := dash.Timeseries("GC STW pause quantiles", dash.UnitSeconds).
		Description("Individual stop-the-world pause durations (two per GC cycle), aggregated across the selection.")
	for _, q := range []struct {
		v float64
		l string
	}{{0.5, "p50"}, {0.99, "p99"}, {0.999, "p99.9"}} {
		p = p.WithTarget(dash.PromQuery(
			fmt.Sprintf(`histogram_quantile(%g, sum by (le) (rate(go_gc_pauses_seconds_bucket{%s}[$__rate_interval])))`, q.v, f), q.l))
	}
	return p
}

func gcPausePerPodPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("GC STW pause p99 by pod", dash.UnitSeconds).
		Description("p99 individual GC stop-the-world pause, one line per pod, so an outlier pod stands out at a glance.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`histogram_quantile(0.99, sum by (le, pod_name) (rate(go_gc_pauses_seconds_bucket{%s}[$__rate_interval])))`, f),
			"{{pod_name}}"))
}

func gcPauseHeatmap() *heatmap.PanelBuilder {
	return dash.Heatmap("GC STW pause distribution", dash.UnitSeconds).
		WithTarget(dash.PromHeatmapQuery(
			fmt.Sprintf(`sum(rate(go_gc_pauses_seconds_bucket{%s}[$__rate_interval])) by (le)`, f)))
}

func gcRatePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("GC cycles/sec", dash.UnitShort).
		Description("GC cycles per second per pod, plus forced cycles (runtime.GC / debug) across the selection.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (rate(go_gc_cycles_total_gc_cycles_total{%s}[$__rate_interval]))`, f), "{{pod_name}}")).
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum(rate(go_gc_cycles_forced_gc_cycles_total{%s}[$__rate_interval]))`, f), "forced (all)"))
}

func gcPacingPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("GC pacing: live vs goal vs limit", dash.UnitBytes).
		Description("Live heap, the GC trigger goal, and GOMEMLIMIT for this pod. If the goal sits well below the limit you are GOGC-bound (raising GOGC trades RAM for fewer GCs); if the goal is pinned at the limit you are GOMEMLIMIT-bound and GC is being forced.").
		WithTarget(dash.PromQuery(fmt.Sprintf(`sum(go_gc_heap_live_bytes{%s})`, f), "live")).
		WithTarget(dash.PromQuery(fmt.Sprintf(`sum(go_gc_heap_goal_bytes{%s})`, f), "goal")).
		WithTarget(dash.PromQuery(fmt.Sprintf(`sum(go_gc_gomemlimit_bytes{%s})`, f), "limit"))
}

// --- Allocation ---

func allocBytesPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Allocation rate (bytes)", dash.UnitBytesPerSec).
		Description("Heap bytes allocated per second, per pod. Drives GC frequency and large-object heap-lock contention.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (rate(go_gc_heap_allocs_bytes_total{%s}[$__rate_interval]))`, f), "{{pod_name}}"))
}

func allocObjectsPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Allocation rate (objects)", dash.UnitOps).
		Description("Heap objects allocated per second, per pod. Drives GC mark/scan cost and allocator overhead more than raw bytes do.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (rate(go_gc_heap_allocs_objects_total{%s}[$__rate_interval]))`, f), "{{pod_name}}"))
}

func avgAllocSizePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Average allocation size", dash.UnitBytes).
		Description("Mean allocated object size (bytes/objects). A drop with rising object rate signals a small-object swarm (scan-cost heavy); a rise signals big-buffer churn (heap-lock heavy).").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (rate(go_gc_heap_allocs_bytes_total{%s}[$__rate_interval])) / sum by (pod_name) (rate(go_gc_heap_allocs_objects_total{%s}[$__rate_interval]))`, f, f),
			"{{pod_name}}"))
}

func scanWorkPanel() *timeseries.PanelBuilder {
	p := dash.StackedTimeseries("Scannable heap (mark work)", dash.UnitBytes).
		Description("Bytes the GC must scan each cycle for this pod, by kind. This is what makes a single GC cycle expensive (pointer-ful live memory).")
	for _, c := range []struct{ metric, legend string }{
		{"go_gc_scan_heap_bytes", "heap"},
		{"go_gc_scan_stack_bytes", "stack"},
		{"go_gc_scan_globals_bytes", "globals"},
	} {
		p = p.WithTarget(dash.PromQuery(fmt.Sprintf(`sum(%s{%s})`, c.metric, f), c.legend))
	}
	return p
}

// --- Memory ---

func memoryByClassPanel() *timeseries.PanelBuilder {
	p := dash.StackedTimeseries("Memory by class", dash.UnitBytes).
		Description("Process memory managed by the Go runtime for this pod, broken down by class (sums to go_memory_classes_total).")
	for _, c := range []struct{ expr, legend string }{
		{`go_memory_classes_heap_objects_bytes`, "heap objects (live)"},
		{`go_memory_classes_heap_unused_bytes`, "heap unused"},
		{`go_memory_classes_heap_free_bytes`, "heap free"},
		{`go_memory_classes_heap_released_bytes`, "heap released"},
		{`go_memory_classes_heap_stacks_bytes + go_memory_classes_os_stacks_bytes`, "stacks"},
		{`go_memory_classes_metadata_mspan_inuse_bytes + go_memory_classes_metadata_mcache_inuse_bytes + go_memory_classes_metadata_other_bytes`, "metadata"},
		{`go_memory_classes_other_bytes + go_memory_classes_profiling_buckets_bytes`, "other"},
	} {
		p = p.WithTarget(dash.PromQuery(fmt.Sprintf(`sum(%s{%s})`, c.expr, f), c.legend))
	}
	return p
}

func liveObjectsPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Live heap objects", dash.UnitShort).
		Description("Count of live heap objects per pod. A leading proxy for GC scan cost and a leak indicator.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (go_gc_heap_objects_objects{%s})`, f), "{{pod_name}}"))
}

// --- Goroutines & scheduler ---

func goroutinesPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Goroutines", dash.UnitShort).
		Description("Total goroutines per pod. Sustained growth often signals pile-up from downstream slowness rather than CPU/GC.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (go_goroutines{%s})`, f), "{{pod_name}}"))
}

func goroutineStatesPanel() *timeseries.PanelBuilder {
	p := dash.StackedTimeseries("Goroutine states", dash.UnitShort).
		Description("Goroutines by scheduler state for this pod. A large 'runnable' bucket means goroutines are ready but waiting for a CPU (CPU starvation).")
	for _, c := range []struct{ metric, legend string }{
		{"go_sched_goroutines_running_goroutines", "running"},
		{"go_sched_goroutines_runnable_goroutines", "runnable"},
		{"go_sched_goroutines_waiting_goroutines", "waiting"},
		{"go_sched_goroutines_not_in_go_goroutines", "not in Go (syscall/cgo)"},
	} {
		p = p.WithTarget(dash.PromQuery(fmt.Sprintf(`sum(%s{%s})`, c.metric, f), c.legend))
	}
	return p
}

func goroutineCreatePanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Goroutine creation rate", dash.UnitOps).
		Description("Goroutines created per second, per pod.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (rate(go_sched_goroutines_created_goroutines_total{%s}[$__rate_interval]))`, f), "{{pod_name}}"))
}

func schedLatencyPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Scheduler latency p99", dash.UnitSeconds).
		Description("p99 time a goroutine waits in the run queue before getting a CPU, per pod. The direct CPU-starvation signal.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`histogram_quantile(0.99, sum by (le, pod_name) (rate(go_sched_latencies_seconds_bucket{%s}[$__rate_interval])))`, f), "{{pod_name}}")).
		Thresholds(dashboard.NewThresholdsConfigBuilder().
			Mode(dashboard.ThresholdsModeAbsolute).
			Steps([]dashboard.Threshold{
				{Value: nil, Color: "green"},
				{Value: new(0.001), Color: "orange"},
				{Value: new(0.01), Color: "red"},
			}))
}

func schedLatencyHeatmap() *heatmap.PanelBuilder {
	return dash.Heatmap("Scheduler latency distribution", dash.UnitSeconds).
		WithTarget(dash.PromHeatmapQuery(
			fmt.Sprintf(`sum(rate(go_sched_latencies_seconds_bucket{%s}[$__rate_interval])) by (le)`, f)))
}

func threadsPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("OS threads", dash.UnitShort).
		Description("OS threads owned by the runtime, per pod. A value far above GOMAXPROCS means many goroutines are blocked in syscalls or cgo calls (each blocking call can pin its own OS thread).").
		WithTarget(dash.PromQuery(fmt.Sprintf(`sum by (pod_name) (go_threads{%s})`, f), "{{pod_name}}"))
}

// --- Contention & stop-the-world ---

func mutexWaitPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Mutex wait rate", dash.UnitShort).
		Description("Seconds of sync.Mutex/RWMutex wait accumulated per second, per pod (~average number of goroutines blocked on application locks). Does not include runtime-internal locks.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (rate(go_sync_mutex_wait_total_seconds_total{%s}[$__rate_interval]))`, f), "{{pod_name}}"))
}

func stwFractionPanel() *timeseries.PanelBuilder {
	return dash.Timeseries("Stop-the-world fraction", dash.UnitPercentUnit).
		Description("Fraction of wall-clock time the program is fully paused, split by cause (GC vs other, e.g. profiling/heap dump). Total pause includes time to stop all Ps.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (rate(go_sched_pauses_total_gc_seconds_sum{%s}[$__rate_interval]))`, f), "{{pod_name}} gc")).
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`sum by (pod_name) (rate(go_sched_pauses_total_other_seconds_sum{%s}[$__rate_interval]))`, f), "{{pod_name}} other"))
}

func stwPauseP99Panel() *timeseries.PanelBuilder {
	return dash.Timeseries("Stop-the-world pause p99", dash.UnitSeconds).
		Description("p99 total stop-the-world pause duration (GC-related), per pod.").
		WithTarget(dash.PromQuery(
			fmt.Sprintf(`histogram_quantile(0.99, sum by (le, pod_name) (rate(go_sched_pauses_total_gc_seconds_bucket{%s}[$__rate_interval])))`, f), "{{pod_name}}"))
}

// --- Template variables ---

func queryVar(name, label, query string, multi bool) *dashboard.QueryVariableBuilder {
	v := dash.QueryVar(name, query).
		Label(label).
		Refresh(dashboard.VariableRefreshOnTimeRangeChanged)
	if multi {
		v = v.Multi(true).IncludeAll(true).AllValue(".*")
	}
	return v
}

// --- Dashboard ---

func build() (dashboard.Dashboard, error) {
	return dashboard.NewDashboardBuilder("Go Runtime").
		Uid("go-runtime").
		Description("Go runtime internals for buildbuddy services: GC CPU and pacing, allocation, scheduler latency, goroutines, memory, and lock contention.").
		Tags([]string{"generated", "go", "runtime", "gc", "file:go-runtime.json"}).
		Editable().
		Refresh("30s").
		Time("now-1h", "now").
		Tooltip(dashboard.DashboardCursorSyncCrosshair).
		WithVariable(queryVar("region", "Region",
			`label_values(go_goroutines, region)`, false).
			Current(dash.SelectedOption("us-west1", "us-west1"))).
		WithVariable(queryVar("job", "Job",
			`label_values(go_goroutines{region=~"$region"}, job)`, false).
			Current(dash.SelectedOption("buildbuddy-app", "buildbuddy-app"))).
		WithVariable(queryVar("pod", "Pod",
			`label_values(go_goroutines{region=~"$region",job=~"$job"}, pod_name)`, true)).
		WithRow(dashboard.NewRowBuilder("CPU")).
		WithPanel(gcCPUFractionPanel()).
		WithRow(dashboard.NewRowBuilder("Garbage collection")).
		WithPanel(gcPauseQuantilesPanel()).
		WithPanel(gcPausePerPodPanel()).
		WithPanel(gcPauseHeatmap()).
		WithPanel(gcRatePanel()).
		WithRow(dashboard.NewRowBuilder("Allocation")).
		WithPanel(allocBytesPanel()).
		WithPanel(allocObjectsPanel()).
		WithPanel(avgAllocSizePanel()).
		WithRow(dashboard.NewRowBuilder("Memory")).
		WithPanel(liveObjectsPanel()).
		WithRow(dashboard.NewRowBuilder("Goroutines & scheduler")).
		WithPanel(goroutinesPanel()).
		WithPanel(goroutineCreatePanel()).
		WithPanel(schedLatencyPanel()).
		WithPanel(schedLatencyHeatmap()).
		WithPanel(threadsPanel()).
		WithRow(dashboard.NewRowBuilder("Contention & stop-the-world")).
		WithPanel(mutexWaitPanel()).
		WithPanel(stwFractionPanel()).
		WithPanel(stwPauseP99Panel()).
		// Collapsed, repeated once per selected pod: a complete single-pod view
		// so you can expand one pod and see everything relevant at a glance. It
		// includes the single-pod-only breakdowns (stacked-by-class / pacing)
		// plus per-pod copies of the key summary signals from the rows above
		// (which $pod scopes to this one pod). Collapsed rows do not run their
		// panel queries until expanded, so this stays cheap with many pods.
		WithRow(dashboard.NewRowBuilder("Per-pod detail ($pod)").
			Collapsed(true).
			Repeat("pod").
			WithPanel(gcCPUFractionPanel()).
			WithPanel(gcCPUByClassPanel()).
			WithPanel(gcPauseQuantilesPanel()).
			WithPanel(gcRatePanel()).
			WithPanel(gcPacingPanel()).
			WithPanel(allocBytesPanel()).
			WithPanel(scanWorkPanel()).
			WithPanel(memoryByClassPanel()).
			WithPanel(cpuByClassPanel()).
			WithPanel(goroutineStatesPanel()).
			WithPanel(schedLatencyPanel()).
			WithPanel(mutexWaitPanel())).
		Build()
}

func main() {
	dash.MustMarshal(build())
}

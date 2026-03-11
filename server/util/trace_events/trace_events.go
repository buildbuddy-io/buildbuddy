// Package trace_events contains Bazel-specific trace profile parsing helpers.
//
// Bazel timing profiles are JSON objects with:
//   - "otherData": build metadata such as build_id, bazel_version, and
//     profile_start_ts.
//   - "traceEvents": a sequence of events in Chrome Trace Event format.
//
// This package focuses on event shapes observed in Bazel profiles (not generic
// arbitrary trace producers). The implementation summarizes complete span
// events (phase "X") and selected counters (for example "action count"), but
// Bazel profiles contain additional useful event types that are important for
// diagnostics and future analysis.
//
// Bazel-specific event phases commonly present:
//   - "M" (metadata): thread_name and thread_sort_index entries, including
//     synthetic threads such as "Main Thread" and "Critical Path".
//   - "X" (complete events): duration spans with category/name pairs such as:
//     "action processing", "remote action execution", "remote action cache
//     check", "gc notification", "critical path component", and others.
//   - "C" (counters): time-series points for resource and throughput signals.
//     Example counter names seen in Bazel profiles include:
//     "CPU usage (Bazel)", "CPU usage (total)", "Memory usage (Bazel)",
//     "Memory usage (total)", "Network Up usage (total)",
//     "Network Down usage (total)", "System load average", and "action count".
//   - "i" (instant): build phase markers such as initialize/evaluate/complete.
//
// Timing units:
//   - ts and dur are microseconds.
//   - ts is relative to profile_start_ts in otherData.
//
// Action and critical-path signals:
//   - "action count" counters provide time-series parallelism information
//     (for example "action" and "local action cache" fields in args).
//   - Critical path is represented by category "critical path component",
//     often emitted on thread 0 ("Critical Path"), and may include provenance
//     such as the originating worker thread in args["tid"].
//
// GC and memory signals:
//   - GC activity appears in categories such as "gc notification" with events
//     like "minor GC".
//   - Memory/cpu/system-load counters can be correlated with long critical-path
//     sections to identify resource pressure.
//
// Trace event format reference:
// https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU
package trace_events

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Phase constants
const (
	PhaseComplete = "X"
	PhaseCounter  = "C"
	DefaultTopN   = 10
)

// Profile represents a trace profile, including all trace events.
type Profile struct {
	TraceEvents []*Event `json:"traceEvents,omitempty"`
}

// Event represents a trace event.
type Event struct {
	Category  string         `json:"cat,omitempty"`
	Name      string         `json:"name,omitempty"`
	Phase     string         `json:"ph,omitempty"`
	Timestamp int64          `json:"ts"`
	Duration  int64          `json:"dur"`
	ProcessID int64          `json:"pid,omitempty"`
	ThreadID  int64          `json:"tid,omitempty"`
	Args      map[string]any `json:"args,omitempty"`
}

type eventWriter struct {
	writer     io.Writer
	wroteFirst bool
}

// NewEventWriter writes a list of TraceEvents as comma-separated JSON objects.
// It does not write the start or end delimiters of the list.
// Each object is written on its own line, which is useful as a delimiter when
// parsing the object in a streaming fashion.
func NewEventWriter(w io.Writer) *eventWriter {
	return &eventWriter{writer: w}
}

// WriteEvent writes the marshaled JSON object on a new line, writing a comma
// first if needed to delimit the previous object.
func (w *eventWriter) WriteEvent(e *Event) error {
	delim := ",\n"
	if !w.wroteFirst {
		delim = "\n"
		w.wroteFirst = true
	}
	if _, err := io.WriteString(w.writer, delim); err != nil {
		return fmt.Errorf("write event delimiter: %w", err)
	}

	b, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	if _, err := w.writer.Write(b); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	return nil
}

var mergedEventsNamePattern = regexp.MustCompile(`(?i)^merged\s+\d+\s+events$`)

type durationAggregate struct {
	DurationUsec int64
	EventCount   int64
}

type threadAggregate struct {
	ID                 int64
	Name               string
	DurationUsec       int64
	CompleteEventCount int64
}

type NamedDuration struct {
	Name                   string  `json:"name"`
	DurationUsec           int64   `json:"duration_usec"`
	EventCount             int64   `json:"event_count"`
	PercentOfTotalDuration float64 `json:"percent_of_total_duration"`
}

type ThreadDuration struct {
	ThreadID               int64   `json:"thread_id"`
	ThreadName             string  `json:"thread_name,omitempty"`
	DurationUsec           int64   `json:"duration_usec"`
	CompleteEventCount     int64   `json:"complete_event_count"`
	PercentOfTotalDuration float64 `json:"percent_of_total_duration"`
}

// SignalDurations summarizes durations of common Bazel-specific signals.
// Durations are microseconds and represent accumulated complete-event time.
type SignalDurations struct {
	CriticalPathUsec           int64 `json:"critical_path_usec"`
	GarbageCollectionUsec      int64 `json:"garbage_collection_usec"`
	MajorGarbageCollectionUsec int64 `json:"major_garbage_collection_usec"`
	ActionProcessingUsec       int64 `json:"action_processing_usec"`
	RemoteActionExecutionUsec  int64 `json:"remote_action_execution_usec"`
	RemoteActionCacheCheckUsec int64 `json:"remote_action_cache_check_usec"`
	LocalActionExecutionUsec   int64 `json:"local_action_execution_usec"`
	QueueingUsec               int64 `json:"queueing_usec"`
}

// ActionCountTimeSeries summarizes Bazel "action count" counter samples.
// Values are sampled from counter events (phase "C") and represent observed
// concurrent actions over time.
type ActionCountTimeSeries struct {
	SampleCount                    int64   `json:"sample_count"`
	ActiveSampleCount              int64   `json:"active_sample_count"`
	AverageActionCount             float64 `json:"average_action_count"`
	AverageActiveActionCount       float64 `json:"average_active_action_count"`
	P90ActionCount                 float64 `json:"p90_action_count"`
	P95ActionCount                 float64 `json:"p95_action_count"`
	MaxActionCount                 float64 `json:"max_action_count"`
	AtOrBelow2ActionsSamplePercent float64 `json:"at_or_below_2_actions_sample_percent"`
}

type CriticalPathComponent struct {
	Name          string `json:"name"`
	TimestampUsec int64  `json:"timestamp_usec"`
	DurationUsec  int64  `json:"duration_usec"`
}

type Summary struct {
	EventCount                  int64                   `json:"event_count"`
	CompleteEventCount          int64                   `json:"complete_event_count"`
	TotalDurationUsec           int64                   `json:"total_duration_usec"`
	TopSpansByDurationUsec      []NamedDuration         `json:"top_spans_by_duration_usec"`
	TopCategoriesByDurationUsec []NamedDuration         `json:"top_categories_by_duration_usec"`
	TopThreadsByDurationUsec    []ThreadDuration        `json:"top_threads_by_duration_usec"`
	CriticalPathComponents      []CriticalPathComponent `json:"critical_path_components,omitempty"`
	// ConfiguredJobs is the configured value of --jobs when known. This is
	// filled by higher-level tooling that has BES command-line context.
	ConfiguredJobs int64 `json:"configured_jobs,omitempty"`
	// ConfiguredJobsSource describes where ConfiguredJobs came from (for
	// example "structured_command_line", "options_parsed", or
	// "inferred_from_host_cpu_count"). Use "unknown" when no source is found.
	ConfiguredJobsSource string                `json:"configured_jobs_source,omitempty"`
	ActionCount          ActionCountTimeSeries `json:"action_count"`
	SignalDurationsUsec  SignalDurations       `json:"signal_durations_usec"`
	HasMergedEvents      bool                  `json:"has_merged_events"`
}

func Summarize(reader io.Reader, topN int) (Summary, error) {
	decoder := json.NewDecoder(reader)

	start, err := decoder.Token()
	if err != nil {
		return Summary{}, err
	}
	startDelim, ok := start.(json.Delim)
	if !ok || startDelim != '{' {
		return Summary{}, fmt.Errorf("expected JSON object")
	}

	durationByName := make(map[string]*durationAggregate)
	durationByCategory := make(map[string]*durationAggregate)
	threadByID := make(map[int64]*threadAggregate)

	var sawTraceEvents bool
	var eventCount int64
	var completeEventCount int64
	var totalDurationUsec int64
	var signalDurations SignalDurations
	var hasMergedEvents bool
	actionCountSamples := make([]float64, 0, 512)
	criticalPathComponents := make([]CriticalPathComponent, 0)

	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return Summary{}, err
		}
		key, ok := keyToken.(string)
		if !ok {
			return Summary{}, fmt.Errorf("expected object key")
		}
		if key != "traceEvents" {
			var skip json.RawMessage
			if err := decoder.Decode(&skip); err != nil {
				return Summary{}, err
			}
			continue
		}

		sawTraceEvents = true
		arrayStart, err := decoder.Token()
		if err != nil {
			return Summary{}, err
		}
		arrayStartDelim, ok := arrayStart.(json.Delim)
		if !ok || arrayStartDelim != '[' {
			return Summary{}, fmt.Errorf("expected traceEvents array")
		}

		for decoder.More() {
			var event Event
			if err := decoder.Decode(&event); err != nil {
				return Summary{}, err
			}
			eventCount++

			thread := threadByID[event.ThreadID]
			if thread == nil {
				thread = &threadAggregate{ID: event.ThreadID}
				threadByID[event.ThreadID] = thread
			}

			if event.Name == "thread_name" {
				if threadName, ok := event.Args["name"].(string); ok && threadName != "" {
					thread.Name = threadName
				}
			}

			if event.Phase == PhaseCounter {
				updateActionCountSamples(event, &actionCountSamples)
				continue
			}
			if event.Phase != PhaseComplete {
				continue
			}

			completeEventCount++
			durationUsec := max(0, event.Duration)
			totalDurationUsec += durationUsec
			thread.DurationUsec += durationUsec
			thread.CompleteEventCount++

			if event.Name != "" {
				agg := durationByName[event.Name]
				if agg == nil {
					agg = &durationAggregate{}
					durationByName[event.Name] = agg
				}
				agg.DurationUsec += durationUsec
				agg.EventCount++
			}

			category := event.Category
			if category == "" {
				category = "<uncategorized>"
			}
			agg := durationByCategory[category]
			if agg == nil {
				agg = &durationAggregate{}
				durationByCategory[category] = agg
			}
			agg.DurationUsec += durationUsec
			agg.EventCount++
			if isCriticalPathEvent(category, event.Name) {
				criticalPathComponents = append(criticalPathComponents, CriticalPathComponent{
					Name:          event.Name,
					TimestampUsec: event.Timestamp,
					DurationUsec:  durationUsec,
				})
			}
			updateSignalDurations(&signalDurations, category, event.Name, durationUsec)
			if mergedEventsNamePattern.MatchString(event.Name) {
				hasMergedEvents = true
			}
		}

		arrayEnd, err := decoder.Token()
		if err != nil {
			return Summary{}, err
		}
		arrayEndDelim, ok := arrayEnd.(json.Delim)
		if !ok || arrayEndDelim != ']' {
			return Summary{}, fmt.Errorf("expected traceEvents array terminator")
		}
	}

	if !sawTraceEvents {
		return Summary{}, fmt.Errorf("profile does not contain traceEvents")
	}
	if _, err := decoder.Token(); err != nil {
		return Summary{}, err
	}

	return Summary{
		EventCount:                  eventCount,
		CompleteEventCount:          completeEventCount,
		TotalDurationUsec:           totalDurationUsec,
		TopSpansByDurationUsec:      topNamedDurations(durationByName, totalDurationUsec, topN),
		TopCategoriesByDurationUsec: topNamedDurations(durationByCategory, totalDurationUsec, topN),
		TopThreadsByDurationUsec:    topThreads(threadByID, totalDurationUsec, topN),
		CriticalPathComponents:      criticalPathComponents,
		ActionCount:                 summarizeActionCountTimeSeries(actionCountSamples),
		SignalDurationsUsec:         signalDurations,
		HasMergedEvents:             hasMergedEvents,
	}, nil
}

func isCriticalPathEvent(category, name string) bool {
	lowerCategory := strings.ToLower(strings.TrimSpace(category))
	lowerName := strings.ToLower(strings.TrimSpace(name))
	return strings.Contains(lowerCategory, "critical path component") || strings.Contains(lowerName, "critical path")
}

func updateSignalDurations(signals *SignalDurations, category, name string, durationUsec int64) {
	if signals == nil || durationUsec <= 0 {
		return
	}
	if isCriticalPathEvent(category, name) {
		signals.CriticalPathUsec += durationUsec
	}
	lowerCategory := strings.ToLower(strings.TrimSpace(category))
	lowerName := strings.ToLower(strings.TrimSpace(name))
	if strings.Contains(lowerCategory, "gc notification") || strings.Contains(lowerName, " gc") || strings.HasSuffix(lowerName, "gc") {
		signals.GarbageCollectionUsec += durationUsec
		if strings.Contains(lowerName, "major gc") {
			signals.MajorGarbageCollectionUsec += durationUsec
		}
	}
	if strings.Contains(lowerCategory, "action processing") || strings.Contains(lowerName, "action processing") {
		signals.ActionProcessingUsec += durationUsec
	}
	if strings.Contains(lowerCategory, "remote action execution") || strings.Contains(lowerName, "remote action execution") {
		signals.RemoteActionExecutionUsec += durationUsec
	}
	if strings.Contains(lowerCategory, "remote action cache check") || strings.Contains(lowerName, "remote action cache check") {
		signals.RemoteActionCacheCheckUsec += durationUsec
	}
	if strings.Contains(lowerCategory, "local action") || strings.Contains(lowerName, "local action") {
		signals.LocalActionExecutionUsec += durationUsec
	}
	if strings.Contains(lowerCategory, "queue") || strings.Contains(lowerName, "queue") {
		signals.QueueingUsec += durationUsec
	}
}

func updateActionCountSamples(event Event, samples *[]float64) {
	if samples == nil {
		return
	}
	if strings.ToLower(strings.TrimSpace(event.Name)) != "action count" {
		return
	}
	value, ok := float64FromAny(event.Args["action"])
	if !ok || value < 0 {
		return
	}
	*samples = append(*samples, value)
}

func summarizeActionCountTimeSeries(samples []float64) ActionCountTimeSeries {
	if len(samples) == 0 {
		return ActionCountTimeSeries{}
	}
	total := 0.0
	maxValue := 0.0
	activeCount := int64(0)
	activeTotal := 0.0
	for _, sample := range samples {
		total += sample
		maxValue = max(maxValue, sample)
		if sample > 0 {
			activeCount++
			activeTotal += sample
		}
	}
	return ActionCountTimeSeries{
		SampleCount:                    int64(len(samples)),
		ActiveSampleCount:              activeCount,
		AverageActionCount:             roundFloat64(total / float64(len(samples))),
		AverageActiveActionCount:       roundFloat64(safeDivide(activeTotal, float64(activeCount))),
		P90ActionCount:                 percentile(samples, 90.0),
		P95ActionCount:                 percentile(samples, 95.0),
		MaxActionCount:                 roundFloat64(maxValue),
		AtOrBelow2ActionsSamplePercent: samplePercentAtOrBelow(samples, DefaultLowParallelismActionCountThreshold),
	}
}

func samplePercentAtOrBelow(samples []float64, threshold float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	count := 0
	for _, sample := range samples {
		if sample <= threshold {
			count++
		}
	}
	return roundFloat64((float64(count) / float64(len(samples))) * 100)
}

func percentile(samples []float64, pct float64) float64 {
	if len(samples) == 0 {
		return 0
	}
	if len(samples) == 1 {
		return roundFloat64(samples[0])
	}
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	ordered := append([]float64(nil), samples...)
	sort.Float64s(ordered)
	position := (pct / 100.0) * float64(len(ordered)-1)
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return roundFloat64(ordered[lower])
	}
	weight := position - float64(lower)
	return roundFloat64(ordered[lower]*(1-weight) + ordered[upper]*weight)
}

func float64FromAny(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int32:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case uint:
		return float64(typed), true
	case uint32:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	case json.Number:
		n, err := typed.Float64()
		return n, err == nil
	case string:
		n, err := strconv.ParseFloat(strings.TrimSpace(typed), 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func safeDivide(numerator, denominator float64) float64 {
	if denominator <= 0 {
		return 0
	}
	return numerator / denominator
}

func roundFloat64(value float64) float64 {
	return math.Round(value*1000) / 1000
}

func topNamedDurations(items map[string]*durationAggregate, totalDurationUsec int64, topN int) []NamedDuration {
	results := make([]NamedDuration, 0, len(items))
	for name, agg := range items {
		results = append(results, NamedDuration{
			Name:                   name,
			DurationUsec:           agg.DurationUsec,
			EventCount:             agg.EventCount,
			PercentOfTotalDuration: percentOfTotal(agg.DurationUsec, totalDurationUsec),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].DurationUsec == results[j].DurationUsec {
			return results[i].Name < results[j].Name
		}
		return results[i].DurationUsec > results[j].DurationUsec
	})
	if topN <= 0 {
		topN = DefaultTopN
	}
	if topN > len(results) {
		topN = len(results)
	}
	return results[:topN]
}

func topThreads(items map[int64]*threadAggregate, totalDurationUsec int64, topN int) []ThreadDuration {
	results := make([]ThreadDuration, 0, len(items))
	for _, agg := range items {
		results = append(results, ThreadDuration{
			ThreadID:               agg.ID,
			ThreadName:             agg.Name,
			DurationUsec:           agg.DurationUsec,
			CompleteEventCount:     agg.CompleteEventCount,
			PercentOfTotalDuration: percentOfTotal(agg.DurationUsec, totalDurationUsec),
		})
	}
	sort.Slice(results, func(i, j int) bool {
		if results[i].DurationUsec == results[j].DurationUsec {
			return results[i].ThreadID < results[j].ThreadID
		}
		return results[i].DurationUsec > results[j].DurationUsec
	})
	if topN <= 0 {
		topN = DefaultTopN
	}
	if topN > len(results) {
		topN = len(results)
	}
	return results[:topN]
}

func percentOfTotal(durationUsec int64, totalDurationUsec int64) float64 {
	if totalDurationUsec <= 0 {
		return 0
	}
	value := (float64(durationUsec) / float64(totalDurationUsec)) * 100
	return roundFloat64(value)
}

func percentOfConfiguredJobs(actionCount float64, configuredJobs int64) float64 {
	if configuredJobs <= 0 || actionCount <= 0 {
		return 0
	}
	return roundFloat64((actionCount / float64(configuredJobs)) * 100)
}

const (
	DefaultHighGCDurationPercentThreshold            = 5.0
	DefaultJobsCriticalPathDominancePercentThreshold = 75.0
	// DefaultJobsUtilizationPercentThreshold marks average action-count
	// utilization below this percentage of configured --jobs as potentially
	// underutilized.
	DefaultJobsUtilizationPercentThreshold    = 60.0
	DefaultQueueingBottleneckPercentThreshold = 20.0
	DefaultLowParallelismActionCountThreshold = 2.0
	DefaultLowParallelismSamplePctThreshold   = 90.0
)

type InsightThresholds struct {
	HighGCDurationPercentThreshold            float64
	JobsCriticalPathDominancePercentThreshold float64
	JobsUtilizationPercentThreshold           float64
	QueueingBottleneckPercentThreshold        float64
}

func (t InsightThresholds) withDefaults() InsightThresholds {
	out := t
	if out.HighGCDurationPercentThreshold <= 0 {
		out.HighGCDurationPercentThreshold = DefaultHighGCDurationPercentThreshold
	}
	if out.JobsCriticalPathDominancePercentThreshold <= 0 {
		out.JobsCriticalPathDominancePercentThreshold = DefaultJobsCriticalPathDominancePercentThreshold
	}
	if out.JobsUtilizationPercentThreshold <= 0 {
		out.JobsUtilizationPercentThreshold = DefaultJobsUtilizationPercentThreshold
	}
	if out.QueueingBottleneckPercentThreshold <= 0 {
		out.QueueingBottleneckPercentThreshold = DefaultQueueingBottleneckPercentThreshold
	}
	return out
}

type TimingProfileInsights struct {
	CriticalPath                    TimingProfileCriticalPathInsight                    `json:"critical_path"`
	GarbageCollection               TimingProfileGarbageCollectionInsight               `json:"garbage_collection"`
	Jobs                            TimingProfileJobsInsight                            `json:"jobs"`
	Bottlenecks                     TimingProfileBottlenecksInsight                     `json:"bottlenecks"`
	LocalActionsWithRemoteExecution TimingProfileLocalActionsWithRemoteExecutionInsight `json:"local_actions_with_remote_execution"`
}

type TimingProfileCriticalPathInsight struct {
	Present                           bool    `json:"present"`
	CriticalPathDurationUsec          int64   `json:"critical_path_duration_usec"`
	CriticalPathShareOfProfileWorkPct float64 `json:"critical_path_share_of_profile_work_percent"`
}

type TimingProfileGarbageCollectionInsight struct {
	GCDurationUsec                       int64   `json:"gc_duration_usec"`
	GCDurationShareOfProfileWorkPct      float64 `json:"gc_share_of_profile_work_percent"`
	MajorGCDurationUsec                  int64   `json:"major_gc_duration_usec"`
	MajorGCDurationShareOfProfileWorkPct float64 `json:"major_gc_share_of_profile_work_percent"`
	HighGCDuration                       bool    `json:"high_gc_duration"`
}

type TimingProfileJobsInsight struct {
	RemoteExecutionDetected                bool    `json:"remote_execution_detected"`
	ConfiguredJobsStatus                   string  `json:"configured_jobs_status"`
	ConfiguredJobs                         int64   `json:"configured_jobs,omitempty"`
	ConfiguredJobsSource                   string  `json:"configured_jobs_source,omitempty"`
	ActionCountSampleCount                 int64   `json:"action_count_sample_count"`
	ActionCountActiveSampleCount           int64   `json:"action_count_active_sample_count"`
	AverageActionCount                     float64 `json:"average_action_count"`
	AverageActiveActionCount               float64 `json:"average_active_action_count"`
	P90ActionCount                         float64 `json:"p90_action_count"`
	P95ActionCount                         float64 `json:"p95_action_count"`
	MaxActionCount                         float64 `json:"max_action_count"`
	AtOrBelow2ActionsSamplePercent         float64 `json:"at_or_below_2_actions_sample_percent"`
	AtOrBelow2ActionsMostOfTime            bool    `json:"at_or_below_2_actions_most_of_time"`
	AverageActionCountVsConfiguredJobsPct  float64 `json:"average_action_count_vs_configured_jobs_percent"`
	AverageActiveCountVsConfiguredJobsPct  float64 `json:"average_active_action_count_vs_configured_jobs_percent"`
	P95ActionCountVsConfiguredJobsPct      float64 `json:"p95_action_count_vs_configured_jobs_percent"`
	MaxActionCountVsConfiguredJobsPct      float64 `json:"max_action_count_vs_configured_jobs_percent"`
	ActionProcessingDurationUsec           int64   `json:"action_processing_duration_usec"`
	CriticalPathDurationUsec               int64   `json:"critical_path_duration_usec"`
	CriticalPathShareOfActionProcessingPct float64 `json:"critical_path_share_of_action_processing_percent"`
	PotentialJobsTuningOpportunity         bool    `json:"potential_jobs_tuning_opportunity"`
}

type TimingProfileBottlenecksInsight struct {
	QueueingDurationUsec          int64   `json:"queueing_duration_usec"`
	QueueingShareOfProfileWorkPct float64 `json:"queueing_share_of_profile_work_percent"`
	PotentialQueueingBottleneck   bool    `json:"potential_queueing_bottleneck"`
}

type TimingProfileLocalActionsWithRemoteExecutionInsight struct {
	RemoteActionExecutionDurationUsec       int64   `json:"remote_action_execution_duration_usec"`
	LocalActionExecutionDurationUsec        int64   `json:"local_action_execution_duration_usec"`
	LocalActionShareOfActionExecutionPct    float64 `json:"local_action_share_of_action_execution_percent"`
	LocalActionsWithRemoteExecutionDetected bool    `json:"local_actions_with_remote_execution_detected"`
}

type TimingProfileDiagnostics struct {
	IncompleteProfile TimingProfileIncompleteProfileDiagnostic `json:"incomplete_profile"`
	MergedEvents      TimingProfileMergedEventsDiagnostic      `json:"merged_events"`
}

type TimingProfileIncompleteProfileDiagnostic struct {
	IsIncomplete bool     `json:"is_incomplete"`
	Reasons      []string `json:"reasons,omitempty"`
}

type TimingProfileMergedEventsDiagnostic struct {
	HasMergedEvents bool `json:"has_merged_events"`
}

type AggregateTimingProfileInsights struct {
	CriticalPath                    AggregateTimingProfileCriticalPathInsight                    `json:"critical_path"`
	GarbageCollection               AggregateTimingProfileGarbageCollectionInsight               `json:"garbage_collection"`
	Jobs                            AggregateTimingProfileJobsInsight                            `json:"jobs"`
	Bottlenecks                     AggregateTimingProfileBottlenecksInsight                     `json:"bottlenecks"`
	LocalActionsWithRemoteExecution AggregateTimingProfileLocalActionsWithRemoteExecutionInsight `json:"local_actions_with_remote_execution"`
}

type AggregateTimingProfileCriticalPathInsight struct {
	InvocationsWithSignalCount               int     `json:"invocations_with_signal_count"`
	TotalCriticalPathDurationUsec            int64   `json:"total_critical_path_duration_usec"`
	AverageCriticalPathDurationUsec          int64   `json:"average_critical_path_duration_usec"`
	AverageCriticalPathShareOfProfileWorkPct float64 `json:"average_critical_path_share_of_profile_work_percent"`
}

type AggregateTimingProfileGarbageCollectionInsight struct {
	InvocationsWithGCCount              int     `json:"invocations_with_gc_count"`
	InvocationsHighGCCount              int     `json:"invocations_high_gc_count"`
	TotalGCDurationUsec                 int64   `json:"total_gc_duration_usec"`
	AverageGCShareOfProfileWorkPct      float64 `json:"average_gc_share_of_profile_work_percent"`
	AverageMajorGCShareOfProfileWorkPct float64 `json:"average_major_gc_share_of_profile_work_percent"`
}

type AggregateTimingProfileJobsInsight struct {
	InvocationsWithRemoteExecutionCount         int     `json:"invocations_with_remote_execution_count"`
	InvocationsWithJobsTuningOpportunityCount   int     `json:"invocations_with_jobs_tuning_opportunity_count"`
	InvocationsWithConfiguredJobsCount          int     `json:"invocations_with_configured_jobs_count"`
	InvocationsWithUnknownConfiguredJobsCount   int     `json:"invocations_with_unknown_configured_jobs_count"`
	AllInvocationsHaveConfiguredJobs            bool    `json:"all_invocations_have_configured_jobs"`
	ConfiguredJobsCoveragePercent               float64 `json:"configured_jobs_coverage_percent"`
	AverageConfiguredJobs                       float64 `json:"average_configured_jobs"`
	AverageActionCountVsConfiguredJobsPct       float64 `json:"average_action_count_vs_configured_jobs_percent"`
	AverageActiveCountVsConfiguredJobsPct       float64 `json:"average_active_action_count_vs_configured_jobs_percent"`
	InvocationsWithActionCountDataCount         int     `json:"invocations_with_action_count_data_count"`
	AverageAtOrBelow2ActionsSamplePercent       float64 `json:"average_at_or_below_2_actions_sample_percent"`
	InvocationsAtOrBelow2ActionsMostOfTimeCount int     `json:"invocations_at_or_below_2_actions_most_of_time_count"`
}

type AggregateTimingProfileBottlenecksInsight struct {
	InvocationsWithQueueingBottleneckCount int     `json:"invocations_with_queueing_bottleneck_count"`
	AverageQueueingShareOfProfileWorkPct   float64 `json:"average_queueing_share_of_profile_work_percent"`
}

type AggregateTimingProfileLocalActionsWithRemoteExecutionInsight struct {
	InvocationsWithLocalActionsWhileRECount int   `json:"invocations_with_local_actions_while_remote_execution_count"`
	TotalLocalActionExecutionDurationUsec   int64 `json:"total_local_action_execution_duration_usec"`
}

type AggregateTimingProfileDiagnostics struct {
	IncompleteProfileCount int `json:"incomplete_profile_count"`
	MergedEventsCount      int `json:"merged_events_count"`
}

type AggregateSummary struct {
	EventCount                  int64           `json:"event_count"`
	CompleteEventCount          int64           `json:"complete_event_count"`
	TotalDurationUsec           int64           `json:"total_duration_usec"`
	AverageDurationUsec         int64           `json:"average_duration_usec"`
	TopSpansByDurationUsec      []NamedDuration `json:"top_spans_by_duration_usec"`
	TopCategoriesByDurationUsec []NamedDuration `json:"top_categories_by_duration_usec"`
}

func SummarizeTimingProfileInsights(summary Summary, thresholds InsightThresholds) TimingProfileInsights {
	thresholds = thresholds.withDefaults()

	signals := summary.SignalDurationsUsec
	actionCount := summary.ActionCount
	configuredJobs := summary.ConfiguredJobs
	criticalPathShareOfProfileWorkPct := percentOfTotal(signals.CriticalPathUsec, summary.TotalDurationUsec)
	gcShareOfProfileWorkPct := percentOfTotal(signals.GarbageCollectionUsec, summary.TotalDurationUsec)
	majorGCShareOfProfileWorkPct := percentOfTotal(signals.MajorGarbageCollectionUsec, summary.TotalDurationUsec)
	criticalPathShareOfActionProcessingPct := percentOfTotal(signals.CriticalPathUsec, signals.ActionProcessingUsec)
	queueingShareOfProfileWorkPct := percentOfTotal(signals.QueueingUsec, summary.TotalDurationUsec)
	localActionShareOfActionExecutionPct := percentOfTotal(
		signals.LocalActionExecutionUsec,
		signals.LocalActionExecutionUsec+signals.RemoteActionExecutionUsec,
	)
	remoteExecutionDetected := signals.RemoteActionExecutionUsec > 0 || signals.RemoteActionCacheCheckUsec > 0

	averageActionCountVsJobsPct := percentOfConfiguredJobs(actionCount.AverageActionCount, configuredJobs)
	averageActiveActionCountVsJobsPct := percentOfConfiguredJobs(actionCount.AverageActiveActionCount, configuredJobs)
	p95ActionCountVsJobsPct := percentOfConfiguredJobs(actionCount.P95ActionCount, configuredJobs)
	maxActionCountVsJobsPct := percentOfConfiguredJobs(actionCount.MaxActionCount, configuredJobs)
	atOrBelow2ActionsMostOfTime := actionCount.SampleCount > 0 &&
		actionCount.AtOrBelow2ActionsSamplePercent >= DefaultLowParallelismSamplePctThreshold
	configuredJobsStatus := "unknown"
	if configuredJobs > 0 {
		configuredJobsStatus = "known"
	}
	potentialJobsTuningOpportunity := false
	if configuredJobs > 0 {
		utilization := averageActiveActionCountVsJobsPct
		if utilization <= 0 {
			utilization = averageActionCountVsJobsPct
		}
		potentialJobsTuningOpportunity = utilization > 0 && utilization < thresholds.JobsUtilizationPercentThreshold
	} else if actionCount.SampleCount > 0 {
		potentialJobsTuningOpportunity = atOrBelow2ActionsMostOfTime
	} else {
		potentialJobsTuningOpportunity = signals.ActionProcessingUsec > 0 &&
			criticalPathShareOfActionProcessingPct > 0 &&
			criticalPathShareOfActionProcessingPct < thresholds.JobsCriticalPathDominancePercentThreshold
	}

	return TimingProfileInsights{
		CriticalPath: TimingProfileCriticalPathInsight{
			Present:                           signals.CriticalPathUsec > 0,
			CriticalPathDurationUsec:          signals.CriticalPathUsec,
			CriticalPathShareOfProfileWorkPct: criticalPathShareOfProfileWorkPct,
		},
		GarbageCollection: TimingProfileGarbageCollectionInsight{
			GCDurationUsec:                       signals.GarbageCollectionUsec,
			GCDurationShareOfProfileWorkPct:      gcShareOfProfileWorkPct,
			MajorGCDurationUsec:                  signals.MajorGarbageCollectionUsec,
			MajorGCDurationShareOfProfileWorkPct: majorGCShareOfProfileWorkPct,
			HighGCDuration: gcShareOfProfileWorkPct >= thresholds.HighGCDurationPercentThreshold ||
				majorGCShareOfProfileWorkPct >= thresholds.HighGCDurationPercentThreshold,
		},
		Jobs: TimingProfileJobsInsight{
			RemoteExecutionDetected:                remoteExecutionDetected,
			ConfiguredJobsStatus:                   configuredJobsStatus,
			ConfiguredJobs:                         configuredJobs,
			ConfiguredJobsSource:                   summary.ConfiguredJobsSource,
			ActionCountSampleCount:                 actionCount.SampleCount,
			ActionCountActiveSampleCount:           actionCount.ActiveSampleCount,
			AverageActionCount:                     actionCount.AverageActionCount,
			AverageActiveActionCount:               actionCount.AverageActiveActionCount,
			P90ActionCount:                         actionCount.P90ActionCount,
			P95ActionCount:                         actionCount.P95ActionCount,
			MaxActionCount:                         actionCount.MaxActionCount,
			AtOrBelow2ActionsSamplePercent:         actionCount.AtOrBelow2ActionsSamplePercent,
			AtOrBelow2ActionsMostOfTime:            atOrBelow2ActionsMostOfTime,
			AverageActionCountVsConfiguredJobsPct:  averageActionCountVsJobsPct,
			AverageActiveCountVsConfiguredJobsPct:  averageActiveActionCountVsJobsPct,
			P95ActionCountVsConfiguredJobsPct:      p95ActionCountVsJobsPct,
			MaxActionCountVsConfiguredJobsPct:      maxActionCountVsJobsPct,
			ActionProcessingDurationUsec:           signals.ActionProcessingUsec,
			CriticalPathDurationUsec:               signals.CriticalPathUsec,
			CriticalPathShareOfActionProcessingPct: criticalPathShareOfActionProcessingPct,
			PotentialJobsTuningOpportunity:         potentialJobsTuningOpportunity,
		},
		Bottlenecks: TimingProfileBottlenecksInsight{
			QueueingDurationUsec:          signals.QueueingUsec,
			QueueingShareOfProfileWorkPct: queueingShareOfProfileWorkPct,
			PotentialQueueingBottleneck:   queueingShareOfProfileWorkPct >= thresholds.QueueingBottleneckPercentThreshold,
		},
		LocalActionsWithRemoteExecution: TimingProfileLocalActionsWithRemoteExecutionInsight{
			RemoteActionExecutionDurationUsec:       signals.RemoteActionExecutionUsec,
			LocalActionExecutionDurationUsec:        signals.LocalActionExecutionUsec,
			LocalActionShareOfActionExecutionPct:    localActionShareOfActionExecutionPct,
			LocalActionsWithRemoteExecutionDetected: remoteExecutionDetected && signals.LocalActionExecutionUsec > 0,
		},
	}
}

func SummarizeTimingProfileDiagnostics(summary Summary) TimingProfileDiagnostics {
	reasons := make([]string, 0, 5)
	if summary.EventCount == 0 {
		reasons = append(reasons, "profile does not contain trace events")
	}
	if summary.CompleteEventCount == 0 {
		reasons = append(reasons, "profile does not contain complete span events")
	}
	if summary.TotalDurationUsec <= 0 {
		reasons = append(reasons, "profile has zero complete-event duration")
	}
	if summary.SignalDurationsUsec.CriticalPathUsec <= 0 {
		reasons = append(reasons, "profile does not include critical path component spans")
	}
	if summary.SignalDurationsUsec.ActionProcessingUsec <= 0 {
		reasons = append(reasons, "profile does not include action processing spans")
	}
	return TimingProfileDiagnostics{
		IncompleteProfile: TimingProfileIncompleteProfileDiagnostic{
			IsIncomplete: len(reasons) > 0,
			Reasons:      reasons,
		},
		MergedEvents: TimingProfileMergedEventsDiagnostic{
			HasMergedEvents: summary.HasMergedEvents,
		},
	}
}

func AggregateSummaryStats(summaries []Summary, topN int) AggregateSummary {
	totalEventCount := int64(0)
	totalCompleteEventCount := int64(0)
	totalDurationUsec := int64(0)
	spanAgg := make(map[string]*durationAggregate)
	categoryAgg := make(map[string]*durationAggregate)

	for _, summary := range summaries {
		totalEventCount += summary.EventCount
		totalCompleteEventCount += summary.CompleteEventCount
		totalDurationUsec += summary.TotalDurationUsec

		for _, span := range summary.TopSpansByDurationUsec {
			agg := spanAgg[span.Name]
			if agg == nil {
				agg = &durationAggregate{}
				spanAgg[span.Name] = agg
			}
			agg.DurationUsec += span.DurationUsec
			agg.EventCount += span.EventCount
		}
		for _, category := range summary.TopCategoriesByDurationUsec {
			agg := categoryAgg[category.Name]
			if agg == nil {
				agg = &durationAggregate{}
				categoryAgg[category.Name] = agg
			}
			agg.DurationUsec += category.DurationUsec
			agg.EventCount += category.EventCount
		}
	}

	return AggregateSummary{
		EventCount:                  totalEventCount,
		CompleteEventCount:          totalCompleteEventCount,
		TotalDurationUsec:           totalDurationUsec,
		AverageDurationUsec:         averageInt64(totalDurationUsec, len(summaries)),
		TopSpansByDurationUsec:      topNamedDurations(spanAgg, totalDurationUsec, topN),
		TopCategoriesByDurationUsec: topNamedDurations(categoryAgg, totalDurationUsec, topN),
	}
}

func AggregateTimingProfileInsightsFromSummaries(summaries []Summary, thresholds InsightThresholds) AggregateTimingProfileInsights {
	criticalPathSignalCount := 0
	totalCriticalPathDurationUsec := int64(0)
	totalCriticalPathSharePct := 0.0

	gcSignalCount := 0
	highGCCount := 0
	totalGCDurationUsec := int64(0)
	totalGCSharePct := 0.0
	totalMajorGCSharePct := 0.0

	jobsRemoteExecutionCount := 0
	jobsOpportunityCount := 0
	jobsConfiguredCount := 0
	totalConfiguredJobs := 0.0
	totalAverageActionCountVsJobsPct := 0.0
	totalAverageActiveCountVsJobsPct := 0.0
	actionCountDataCount := 0
	totalAtOrBelow2ActionsSamplePct := 0.0
	atOrBelow2ActionsMostOfTimeCount := 0

	queueingBottleneckCount := 0
	totalQueueingSharePct := 0.0

	localActionsWhileRECount := 0
	totalLocalActionExecutionDurationUsec := int64(0)

	for _, summary := range summaries {
		insights := SummarizeTimingProfileInsights(summary, thresholds)

		if insights.CriticalPath.Present {
			criticalPathSignalCount++
			totalCriticalPathDurationUsec += insights.CriticalPath.CriticalPathDurationUsec
			totalCriticalPathSharePct += insights.CriticalPath.CriticalPathShareOfProfileWorkPct
		}

		if insights.GarbageCollection.GCDurationUsec > 0 {
			gcSignalCount++
		}
		if insights.GarbageCollection.HighGCDuration {
			highGCCount++
		}
		totalGCDurationUsec += insights.GarbageCollection.GCDurationUsec
		totalGCSharePct += insights.GarbageCollection.GCDurationShareOfProfileWorkPct
		totalMajorGCSharePct += insights.GarbageCollection.MajorGCDurationShareOfProfileWorkPct

		if insights.Jobs.RemoteExecutionDetected {
			jobsRemoteExecutionCount++
		}
		if insights.Jobs.PotentialJobsTuningOpportunity {
			jobsOpportunityCount++
		}
		if insights.Jobs.ConfiguredJobs > 0 {
			jobsConfiguredCount++
			totalConfiguredJobs += float64(insights.Jobs.ConfiguredJobs)
			totalAverageActionCountVsJobsPct += insights.Jobs.AverageActionCountVsConfiguredJobsPct
			totalAverageActiveCountVsJobsPct += insights.Jobs.AverageActiveCountVsConfiguredJobsPct
		}
		if insights.Jobs.ActionCountSampleCount > 0 {
			actionCountDataCount++
			totalAtOrBelow2ActionsSamplePct += insights.Jobs.AtOrBelow2ActionsSamplePercent
		}
		if insights.Jobs.AtOrBelow2ActionsMostOfTime {
			atOrBelow2ActionsMostOfTimeCount++
		}

		if insights.Bottlenecks.PotentialQueueingBottleneck {
			queueingBottleneckCount++
		}
		totalQueueingSharePct += insights.Bottlenecks.QueueingShareOfProfileWorkPct

		if insights.LocalActionsWithRemoteExecution.LocalActionsWithRemoteExecutionDetected {
			localActionsWhileRECount++
		}
		totalLocalActionExecutionDurationUsec += insights.LocalActionsWithRemoteExecution.LocalActionExecutionDurationUsec
	}

	return AggregateTimingProfileInsights{
		CriticalPath: AggregateTimingProfileCriticalPathInsight{
			InvocationsWithSignalCount:               criticalPathSignalCount,
			TotalCriticalPathDurationUsec:            totalCriticalPathDurationUsec,
			AverageCriticalPathDurationUsec:          averageInt64(totalCriticalPathDurationUsec, criticalPathSignalCount),
			AverageCriticalPathShareOfProfileWorkPct: averageFloat64(totalCriticalPathSharePct, criticalPathSignalCount),
		},
		GarbageCollection: AggregateTimingProfileGarbageCollectionInsight{
			InvocationsWithGCCount:              gcSignalCount,
			InvocationsHighGCCount:              highGCCount,
			TotalGCDurationUsec:                 totalGCDurationUsec,
			AverageGCShareOfProfileWorkPct:      averageFloat64(totalGCSharePct, len(summaries)),
			AverageMajorGCShareOfProfileWorkPct: averageFloat64(totalMajorGCSharePct, len(summaries)),
		},
		Jobs: AggregateTimingProfileJobsInsight{
			InvocationsWithRemoteExecutionCount:         jobsRemoteExecutionCount,
			InvocationsWithJobsTuningOpportunityCount:   jobsOpportunityCount,
			InvocationsWithConfiguredJobsCount:          jobsConfiguredCount,
			InvocationsWithUnknownConfiguredJobsCount:   len(summaries) - jobsConfiguredCount,
			AllInvocationsHaveConfiguredJobs:            len(summaries) > 0 && jobsConfiguredCount == len(summaries),
			ConfiguredJobsCoveragePercent:               percentOfTotal(int64(jobsConfiguredCount), int64(len(summaries))),
			AverageConfiguredJobs:                       averageFloat64(totalConfiguredJobs, jobsConfiguredCount),
			AverageActionCountVsConfiguredJobsPct:       averageFloat64(totalAverageActionCountVsJobsPct, jobsConfiguredCount),
			AverageActiveCountVsConfiguredJobsPct:       averageFloat64(totalAverageActiveCountVsJobsPct, jobsConfiguredCount),
			InvocationsWithActionCountDataCount:         actionCountDataCount,
			AverageAtOrBelow2ActionsSamplePercent:       averageFloat64(totalAtOrBelow2ActionsSamplePct, actionCountDataCount),
			InvocationsAtOrBelow2ActionsMostOfTimeCount: atOrBelow2ActionsMostOfTimeCount,
		},
		Bottlenecks: AggregateTimingProfileBottlenecksInsight{
			InvocationsWithQueueingBottleneckCount: queueingBottleneckCount,
			AverageQueueingShareOfProfileWorkPct:   averageFloat64(totalQueueingSharePct, len(summaries)),
		},
		LocalActionsWithRemoteExecution: AggregateTimingProfileLocalActionsWithRemoteExecutionInsight{
			InvocationsWithLocalActionsWhileRECount: localActionsWhileRECount,
			TotalLocalActionExecutionDurationUsec:   totalLocalActionExecutionDurationUsec,
		},
	}
}

func AggregateTimingProfileDiagnosticsFromSummaries(summaries []Summary) AggregateTimingProfileDiagnostics {
	incompleteProfileCount := 0
	mergedEventsCount := 0
	for _, summary := range summaries {
		diagnostics := SummarizeTimingProfileDiagnostics(summary)
		if diagnostics.IncompleteProfile.IsIncomplete {
			incompleteProfileCount++
		}
		if diagnostics.MergedEvents.HasMergedEvents {
			mergedEventsCount++
		}
	}
	return AggregateTimingProfileDiagnostics{
		IncompleteProfileCount: incompleteProfileCount,
		MergedEventsCount:      mergedEventsCount,
	}
}

func averageInt64(total int64, count int) int64 {
	if count <= 0 {
		return 0
	}
	return total / int64(count)
}

func averageFloat64(total float64, count int) float64 {
	if count <= 0 {
		return 0
	}
	value := total / float64(count)
	return math.Round(value*1000) / 1000
}

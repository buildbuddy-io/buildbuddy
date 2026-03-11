package mcptools

import (
	"slices"
	"testing"

	analyzeprofileschema "github.com/buildbuddy-io/buildbuddy/cli/mcp/mcptools/analyzeprofileschema"
	"github.com/buildbuddy-io/buildbuddy/server/util/trace_events"
)

func TestProjectedSummaryForOutput_DefaultCompact(t *testing.T) {
	summary := &trace_events.Summary{
		EventCount:                  10,
		CompleteEventCount:          9,
		TotalDurationUsec:           1234,
		TopSpansByDurationUsec:      []trace_events.NamedDuration{{Name: "span", DurationUsec: 10}},
		TopCategoriesByDurationUsec: []trace_events.NamedDuration{{Name: "cat", DurationUsec: 11}},
		TopThreadsByDurationUsec:    []trace_events.ThreadDuration{{ThreadID: 1, DurationUsec: 12}},
		CriticalPathComponents:      []trace_events.CriticalPathComponent{{Name: "cp", DurationUsec: 13}},
	}
	got := projectedSummaryForOutput(summary, analyzeProfileResponseOptions{})
	if got != nil {
		t.Fatalf("projectedSummaryForOutput() = %#v, want nil when include_summary=false", got)
	}
}

func TestProjectedSummaryForOutput_SelectedSections(t *testing.T) {
	summary := &trace_events.Summary{
		EventCount:                  10,
		CompleteEventCount:          9,
		TotalDurationUsec:           1234,
		TopSpansByDurationUsec:      []trace_events.NamedDuration{{Name: "span", DurationUsec: 10}},
		TopCategoriesByDurationUsec: []trace_events.NamedDuration{{Name: "cat", DurationUsec: 11}},
		TopThreadsByDurationUsec:    []trace_events.ThreadDuration{{ThreadID: 1, DurationUsec: 12}},
		CriticalPathComponents:      []trace_events.CriticalPathComponent{{Name: "cp", DurationUsec: 13}},
	}
	got := projectedSummaryForOutput(summary, analyzeProfileResponseOptions{
		IncludeSummary:              true,
		IncludeSummaryTopSpans:      true,
		IncludeSummaryTopCategories: false,
		IncludeSummaryTopThreads:    true,
	})
	if got == nil {
		t.Fatalf("projectedSummaryForOutput() = nil, want non-nil")
		return
	}
	if len(got.TopSpansByDurationUsec) != 1 {
		t.Fatalf("top spans len = %d, want 1", len(got.TopSpansByDurationUsec))
	}
	if got.TopCategoriesByDurationUsec != nil {
		t.Fatalf("top categories = %#v, want nil", got.TopCategoriesByDurationUsec)
	}
	if len(got.TopThreadsByDurationUsec) != 1 {
		t.Fatalf("top threads len = %d, want 1", len(got.TopThreadsByDurationUsec))
	}
	if got.CriticalPathComponents != nil {
		t.Fatalf("critical path = %#v, want nil", got.CriticalPathComponents)
	}
}

func TestApplyAnalyzeProfileOutputLimit_Truncates(t *testing.T) {
	criticalPath := make([]trace_events.CriticalPathComponent, 0, 200)
	for i := range 200 {
		criticalPath = append(criticalPath, trace_events.CriticalPathComponent{
			Name:          "critical path component that is intentionally long for payload size testing",
			TimestampUsec: int64(i),
			DurationUsec:  10_000,
		})
	}
	response := &analyzeprofileschema.Response{
		RequestedInvocationCount: 1,
		AnalyzedInvocationCount:  1,
		Invocations: []analyzeprofileschema.InvocationResult{
			{
				InvocationID: "abc",
				Status:       "ok",
				Summary: &trace_events.Summary{
					EventCount:             10,
					CompleteEventCount:     9,
					TotalDurationUsec:      1234,
					CriticalPathComponents: criticalPath,
				},
				Hints: []analyzeprofileschema.Hint{
					{
						Message: "hint",
						Tool:    "get_executions",
						GetExecutions: &analyzeprofileschema.GetExecutionsHintArguments{
							InvocationID: "abc",
							TargetLabel:  "//foo:bar",
						},
					},
				},
			},
		},
	}
	outputBytesBefore := setResponseOutputBytes(response)

	got := applyAnalyzeProfileOutputLimit(response, 700)
	if got == nil {
		t.Fatalf("applyAnalyzeProfileOutputLimit() = nil")
		return
	}
	truncation := got.Truncation
	if truncation == nil {
		t.Fatalf("truncation = nil, want non-nil")
		return
	}
	if got.OutputBytes >= outputBytesBefore {
		t.Fatalf("output_bytes = %d, want less than pre-truncation size %d", got.OutputBytes, outputBytesBefore)
	}
	if !slices.Contains(truncation.DroppedFields, "invocations[].summary.critical_path_components") {
		t.Fatalf("dropped_fields = %v, want critical path section drop", truncation.DroppedFields)
	}
}

func TestAggregateTimingProfileResults_DefaultsToCompactInvocationPayload(t *testing.T) {
	summary := &trace_events.Summary{
		EventCount:         11,
		CompleteEventCount: 10,
		TotalDurationUsec:  9876,
		TopSpansByDurationUsec: []trace_events.NamedDuration{
			{Name: "Action.execute", DurationUsec: 100},
		},
		TopCategoriesByDurationUsec: []trace_events.NamedDuration{
			{Name: "action processing", DurationUsec: 100},
		},
		TopThreadsByDurationUsec: []trace_events.ThreadDuration{
			{ThreadID: 37, ThreadName: "Main Thread", DurationUsec: 100},
		},
		CriticalPathComponents: []trace_events.CriticalPathComponent{
			{Name: "action '//foo:bar'", DurationUsec: 100},
		},
	}
	results := []analyzeprofileschema.InvocationResult{
		{
			InvocationID: "inv1",
			Status:       "ok",
			Summary:      summary,
		},
	}

	got := aggregateTimingProfileResults(
		[]string{"inv1"},
		10,
		100,
		results,
		analyzeProfileResponseOptions{
			MaxOutputBytes: 64 * 1024,
		},
	)
	if got == nil {
		t.Fatalf("aggregateTimingProfileResults() = nil")
		return
	}
	if len(got.Invocations) != 1 {
		t.Fatalf("invocation count = %d, want 1", len(got.Invocations))
		return
	}
	invocation := got.Invocations[0]
	if invocation.Summary != nil {
		t.Fatalf("summary = %#v, want nil in compact default mode", invocation.Summary)
	}
	if invocation.Metrics == nil {
		t.Fatalf("metrics = nil, want non-nil")
		return
	}
	if invocation.Metrics.EventCount != summary.EventCount {
		t.Fatalf("metrics.event_count = %d, want %d", invocation.Metrics.EventCount, summary.EventCount)
	}
}

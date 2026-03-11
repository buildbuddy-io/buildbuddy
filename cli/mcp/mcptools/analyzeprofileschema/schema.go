package analyzeprofileschema

import (
	"github.com/buildbuddy-io/buildbuddy/server/util/trace_events"
)

// Response models the JSON payload returned by the MCP analyze_profile tool.
type Response struct {
	// RequestedInvocationCount is the number of unique invocation IDs requested.
	RequestedInvocationCount int `json:"requested_invocation_count"`
	// AnalyzedInvocationCount is the number of invocations that finished analysis successfully.
	AnalyzedInvocationCount int `json:"analyzed_invocation_count"`
	// FailedInvocationCount is the number of invocations that returned an analysis error.
	FailedInvocationCount int `json:"failed_invocation_count"`
	// TopN is the applied limit for "top_*" summary lists.
	TopN int `json:"top_n"`
	// Parallelism is the applied max concurrency for phase-2 remote-execution
	// analysis actions (it does not control invocation/BES preparation fanout).
	Parallelism int `json:"parallelism"`
	// MaxOutputBytes is the applied maximum JSON payload size cap.
	MaxOutputBytes int `json:"max_output_bytes,omitempty"`
	// OutputBytes is the JSON payload size after any truncation.
	OutputBytes int `json:"output_bytes"`
	// Truncation is present when the response was trimmed to fit MaxOutputBytes.
	Truncation *OutputTruncation `json:"truncation,omitempty"`
	// Invocations contains one result per requested invocation ID.
	Invocations []InvocationResult `json:"invocations"`
	// Insights aggregates derived signals across successful invocation summaries.
	Insights trace_events.AggregateTimingProfileInsights `json:"insights"`
	// Diagnostics aggregates profile quality diagnostics across successful summaries.
	Diagnostics AggregateTimingProfileDiagnostics `json:"diagnostics"`
	// Aggregate contains rolled-up raw summary stats (counts, durations, top spans/categories).
	Aggregate trace_events.AggregateSummary `json:"aggregate"`
}

// InvocationResult is the per-invocation result emitted by analyze_profile.
type InvocationResult struct {
	// InvocationID is the canonical invocation ID (UUID).
	InvocationID string `json:"invocation_id"`
	// Status is "ok" or "error".
	Status string `json:"status"`
	// Error is present when Status == "error".
	Error string `json:"error,omitempty"`
	// ProfileFile describes the timing-profile artifact chosen from BES outputs.
	ProfileFile *ProfileFile `json:"profile_file,omitempty"`
	// ProfileIsGzip indicates whether the profile bytes were gzip-compressed.
	ProfileIsGzip bool `json:"profile_is_gzip,omitempty"`
	// Metrics is a compact, always-useful per-invocation KPI bundle.
	Metrics *InvocationMetrics `json:"metrics,omitempty"`
	// Summary is the parsed timing-profile summary for successful analyses.
	// This is omitted by default unless include_summary is set.
	Summary *trace_events.Summary `json:"summary,omitempty"`
	// Insights are per-invocation derived signals computed from Summary.
	Insights *trace_events.TimingProfileInsights `json:"insights,omitempty"`
	// Diagnostics are per-invocation profile quality signals (for example, incomplete profile).
	Diagnostics *trace_events.TimingProfileDiagnostics `json:"diagnostics,omitempty"`
	// RemoteExecution captures metadata about the RE action used for this analysis.
	RemoteExecution *RemoteExecution `json:"remote_execution,omitempty"`
	// Hints contains optional follow-up suggestions (for example get_executions
	// calls for slow targets) when analyze_profile is invoked with hints=true.
	Hints []Hint `json:"hints,omitempty"`
}

// InvocationMetrics contains compact per-invocation metrics suitable for
// default agent workflows.
type InvocationMetrics struct {
	// EventCount is the number of events decoded from traceEvents.
	EventCount int64 `json:"event_count"`
	// CompleteEventCount is the number of complete-duration ("X") events.
	CompleteEventCount int64 `json:"complete_event_count"`
	// TotalDurationUsec is the sum of complete-event durations.
	TotalDurationUsec int64 `json:"total_duration_usec"`
	// ConfiguredJobs is the configured --jobs value when known.
	ConfiguredJobs int64 `json:"configured_jobs,omitempty"`
	// ConfiguredJobsSource describes where ConfiguredJobs came from.
	ConfiguredJobsSource string `json:"configured_jobs_source,omitempty"`
	// ActionCount summarizes observed concurrent actions over time.
	ActionCount trace_events.ActionCountTimeSeries `json:"action_count"`
	// SignalDurationsUsec summarizes key duration-based bottleneck signals.
	SignalDurationsUsec trace_events.SignalDurations `json:"signal_durations_usec"`
	// HasMergedEvents indicates whether synthetic "merged N events" entries were present.
	HasMergedEvents bool `json:"has_merged_events"`
}

// OutputTruncation describes payload trimming applied to satisfy MaxOutputBytes.
type OutputTruncation struct {
	// OutputBytesBefore is the JSON payload size before truncation.
	OutputBytesBefore int `json:"output_bytes_before"`
	// OutputBytesAfter is the JSON payload size after truncation.
	OutputBytesAfter int `json:"output_bytes_after"`
	// DroppedFields lists fields removed to shrink the payload.
	DroppedFields []string `json:"dropped_fields"`
}

// ProfileFile describes timing-profile file metadata embedded in invocation
// results.
type ProfileFile struct {
	// Path is the slash-joined BES path_prefix + name.
	Path string `json:"path"`
	Name string `json:"name"`
	// URI is the BES-provided bytestream URI for the profile artifact.
	URI string `json:"uri"`
	// Digest is the raw BES digest proto object (schema intentionally left flexible).
	Digest any `json:"digest,omitempty"`
	// LengthBytes is the profile artifact length from BES metadata.
	LengthBytes int64 `json:"length_bytes"`
	// ResourceName is the CAS download resource name parsed from URI.
	ResourceName string `json:"resource_name,omitempty"`
	// SymlinkTargetPath is set when BES reports this file as a symlink.
	SymlinkTargetPath string `json:"symlink_target_path,omitempty"`
	// InlinedContentsBytes reports embedded content length when BES inlines the file.
	InlinedContentsBytes int `json:"inlined_contents_bytes,omitempty"`
}

// RemoteExecution contains metadata about the RE action used for analysis.
type RemoteExecution struct {
	// InstanceName is the RE/CAS instance name used for the action.
	InstanceName string `json:"instance_name"`
	// DigestFunction is the digest algorithm (for example, SHA256 or BLAKE3).
	DigestFunction string `json:"digest_function"`
	// ActionResourceName is the action digest resource name, useful for debugging.
	ActionResourceName string `json:"action_resource_name"`
	// ExitCode is the analyzer process exit code from the RE result.
	ExitCode int `json:"exit_code"`
}

// Hint is an actionable follow-up suggestion attached to an invocation.
type Hint struct {
	// Message is human-readable guidance for the follow-up step.
	Message string `json:"message"`
	// Tool is the MCP tool name to call for follow-up, when applicable.
	Tool string `json:"tool,omitempty"`
	// GetExecutions provides suggested arguments when Tool == "get_executions".
	GetExecutions *GetExecutionsHintArguments `json:"get_executions,omitempty"`
}

// GetExecutionsHintArguments are suggested MCP args for get_executions.
type GetExecutionsHintArguments struct {
	// InvocationID is the invocation to inspect.
	InvocationID string `json:"invocation_id"`
	// TargetLabel narrows get_executions to a specific target.
	TargetLabel string `json:"target_label,omitempty"`
}

// AggregateTimingProfileDiagnostics is the aggregate diagnostics payload
// returned by analyze_profile.
type AggregateTimingProfileDiagnostics struct {
	// IncompleteProfile counts/samples invocations whose profile data looked incomplete.
	IncompleteProfile AggregateTimingProfileIncompleteProfileDiagnostic `json:"incomplete_profile"`
	// MergedEvents counts/samples invocations that contained synthetic merged events.
	MergedEvents AggregateTimingProfileMergedEventsDiagnostic `json:"merged_events"`
}

type AggregateTimingProfileIncompleteProfileDiagnostic struct {
	// IncompleteInvocationCount is the number of analyzed invocations flagged as incomplete.
	IncompleteInvocationCount int `json:"incomplete_invocation_count"`
	// SampleInvocationIDs is a bounded sample of invocation IDs flagged as incomplete.
	SampleInvocationIDs []string `json:"sample_invocation_ids,omitempty"`
}

type AggregateTimingProfileMergedEventsDiagnostic struct {
	// MergedEventsInvocationCount is the number of analyzed invocations with merged events.
	MergedEventsInvocationCount int `json:"merged_events_invocation_count"`
	// SampleInvocationIDs is a bounded sample of invocation IDs containing merged events.
	SampleInvocationIDs []string `json:"sample_invocation_ids,omitempty"`
}

package sku

import (
	"strings"
)

// SKU is a unique, human-readable identifier tracking a specific usage count.
// This is stored in the OLAP database as a low-cardinality string.
// IMPORTANT: Do not cast arbitrary strings to this SKU type - use one of the
// constants defined below.
type SKU string

func (s SKU) String() string {
	return string(s)
}

// SKU constants - enumerated explicitly to ensure low cardinality.
//
// The format is roughly "<product-area>.<category>.<metric>", organized mostly
// according to the originating RPC. This hierarchical convention allows easily
// querying subsets of usage data using prefix matching, and when sorting by
// SKU, the usage data is naturally grouped by service and type.
const (
	// SKUs related to build event handling/processing (PublishBuildToolEventStream RPC)
	categoryBES = "build_events.build_event_stream."

	BuildEventsBESCount SKU = categoryBES + "count"

	// SKUs related to remote cache (Content Addressable Storage / CAS RPCs)
	categoryCAS = "remote_cache.content_addressable_storage."

	RemoteCacheCASHits            SKU = categoryCAS + "hits"
	RemoteCacheCASDownloadedBytes SKU = categoryCAS + "downloaded_bytes"
	RemoteCacheCASUploadedBytes   SKU = categoryCAS + "uploaded_bytes"

	// SKUs related to remote cache (Action Cache / AC RPCs)
	categoryAC = "remote_cache.action_cache."

	RemoteCacheACHits                    SKU = categoryAC + "hits"
	RemoteCacheACCachedExecDurationNanos SKU = categoryAC + "cached_execution_duration_nanos"

	// SKUs related to remote execution action processing (Execute RPC)
	categoryRBE = "remote_execution.execute."

	RemoteExecutionExecuteWorkerDurationNanos      SKU = categoryRBE + "worker_duration_nanos"
	RemoteExecutionExecuteWorkerCPUNanos           SKU = categoryRBE + "worker_cpu_nanos"
	RemoteExecutionExecuteWorkerMemoryGBNanos      SKU = categoryRBE + "worker_memory_gb_nanos"
	RemoteExecutionExecuteFixedComputeNanos        SKU = categoryRBE + "fixed_compute_nanos"
	RemoteExecutionExecuteFlexibleComputeNanos     SKU = categoryRBE + "flexible_compute_nanos"
	RemoteExecutionExecuteLocalSnapshotSavedBytes  SKU = categoryRBE + "local_snapshot_saved_bytes"
	RemoteExecutionExecuteRemoteSnapshotSavedBytes SKU = categoryRBE + "remote_snapshot_saved_bytes"
)

// LabelName is a usage counter label, which further qualifies the SKU.
//
// TODO: make this a type instead of type alias. GORM's ClickHouse plugin
// doesn't support automatic conversion of `map[LabelName][LabelValue]` to
// `map[string]string`.
type LabelName = string

// Label name constants - enumerated explicitly to ensure low cardinality.
const (
	// Client identifies the type of client that generated the usage, such as
	// "bazel" or "executor".
	Client LabelName = "client"
	// Server identifies the type of server that ultimately handled generating
	// the response, for example "cache-proxy" or "app".
	Server LabelName = "server"
	// Origin identifies internal vs. external traffic origin.
	Origin LabelName = "origin"
	// OS identifies the operating system for execution usage.
	OS LabelName = "os"
	// Arch identifies the machine architecture for execution usage.
	Arch LabelName = "arch"
	// SelfHosted indicates whether the usage was incurred on a self-hosted
	// instance of the service.
	SelfHosted LabelName = "self_hosted"
	// IsolationType is the effective (lower case) isolation type that ran the
	// execution (oci, firecracker, etc.).
	IsolationType LabelName = "isolation_type"
	// TODO: client region (if known), server region
)

// LabelValue is the value of a label.
//
// TODO: make this a type instead of type alias. GORM's ClickHouse plugin
// doesn't support automatic conversion of `map[LabelName][LabelValue]` to
// `map[string]string`.
type LabelValue = string

// Label name values - enumerated explicitly to ensure low cardinality.
const (
	// UnknownLabelValue is the value used when a label could not be parsed from
	// client-provided info. For example, if we unexpectedly see an unsupported
	// executor OS, we would apply the label "os": "unknown" to the execution
	// usage counts.
	UnknownLabelValue LabelValue = "unknown"
	// UnsetLabelValue is the label value used when a label is expected but is
	// missing.
	UnsetLabelValue LabelValue = ""

	ClientApp               LabelValue = "app"
	ClientBazel             LabelValue = "bazel"
	ClientExecutor          LabelValue = "executor"
	ClientExecutorWorkflows LabelValue = "executor-workflows"
	ServerApp               LabelValue = "app"
	ServerCacheProxy        LabelValue = "cache-proxy"
	OriginExternal          LabelValue = "external"
	OriginInternal          LabelValue = "internal"
	OSLinux                 LabelValue = "linux"
	OSMac                   LabelValue = "mac"
	OSWindows               LabelValue = "windows"
	ArchX86_64              LabelValue = "x86_64"
	ArchArm64               LabelValue = "arm64"
	SelfHostedFalse         LabelValue = "false"
	SelfHostedTrue          LabelValue = "true"
)

// GetOSLabel returns the low-cardinality OS label for an execution platform OS.
func GetOSLabel(os string) LabelValue {
	switch strings.TrimSpace(strings.ToLower(os)) {
	case "":
		// Linux is the default execution platform if unset.
		return OSLinux
	case "linux":
		return OSLinux
	case "darwin", "mac":
		return OSMac
	case "windows":
		return OSWindows
	default:
		return UnknownLabelValue
	}
}

// GetArchLabel returns the low-cardinality arch label for an execution platform arch.
func GetArchLabel(arch string) LabelValue {
	switch strings.TrimSpace(strings.ToLower(arch)) {
	case "":
		// x86_64 is the default execution platform arch if unset.
		return ArchX86_64
	case "amd64", "x86_64":
		return ArchX86_64
	case "arm64", "aarch64":
		return ArchArm64
	default:
		return UnknownLabelValue
	}
}

// GetSelfHostedLabel returns the low-cardinality self-hosted label.
func GetSelfHostedLabel(isSelfHosted bool) LabelValue {
	if isSelfHosted {
		return SelfHostedTrue
	}
	return SelfHostedFalse
}

func GetIsolationTypeLabel(isolationType string) LabelValue {
	return strings.ToLower(isolationType)
}

// Labels represents a collection of unique label values.
type Labels = map[LabelName]LabelValue

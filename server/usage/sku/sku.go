package sku

import "strings"

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
// The format is roughly "<service>.<category>.<metric>". This hierarchical
// convention allows easily querying subsets of usage data using prefix
// matching, and when sorting by SKU, the usage data is naturally grouped by
// service and type.
const (
	BuildEventsBESCount SKU = "build_events.build_event_stream.count"

	RemoteCacheCASHits                   SKU = "remote_cache.content_addressable_storage.hits"
	RemoteCacheCASDownloadedBytes        SKU = "remote_cache.content_addressable_storage.downloaded_bytes"
	RemoteCacheCASUploadedBytes          SKU = "remote_cache.content_addressable_storage.uploaded_bytes"
	RemoteCacheACHits                    SKU = "remote_cache.action_cache_hits.hits"
	RemoteCacheACCachedExecDurationNanos SKU = "remote_cache.action_cache.cached_execution_duration_nanos"

	RemoteExecutionExecuteWorkerDurationNanos SKU = "remote_execution.execute.worker_duration_nanos"
	RemoteExecutionExecuteWorkerCPUNanos      SKU = "remote_execution.execute.worker_cpu_nanos"
	RemoteExecutionExecuteWorkerMemoryGBNanos SKU = "remote_execution.execute.worker_memory_gb_nanos"
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
	// SelfHosted indicates whether the usage was incurred on a self-hosted
	// instance of the service.
	SelfHosted LabelName = "self_hosted"
	// TODO: executor arch, client region (if known), server region
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

	ClientApp        LabelValue = "app"
	ClientBazel      LabelValue = "bazel"
	ClientExecutor   LabelValue = "executor"
	ServerApp        LabelValue = "app"
	ServerCacheProxy LabelValue = "cache-proxy"
	OriginExternal   LabelValue = "external"
	OriginInternal   LabelValue = "internal"
	OSLinux          LabelValue = "linux"
	OSMac            LabelValue = "mac"
	OSWindows        LabelValue = "windows"
	SelfHostedFalse  LabelValue = "false"
	SelfHostedTrue   LabelValue = "true"
)

func GetOSLabel(os string) LabelValue {
	switch strings.TrimSpace(strings.ToLower(os)) {
	case "":
		// Linux is the default execution platform if unset.
		return OSLinux
	case "linux":
		return OSLinux
	case "mac":
		return OSMac
	case "windows":
		return OSWindows
	default:
		return UnknownLabelValue
	}
}

func GetSelfHostedLabel(isSelfHosted bool) LabelValue {
	if isSelfHosted {
		return SelfHostedTrue
	}
	return SelfHostedFalse
}

// Labels represents a collection of unique label values.
type Labels = map[LabelName]LabelValue

package mcptools

import (
	"context"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	schemaTypeObject  = "object"
	schemaTypeString  = "string"
	schemaTypeInteger = "integer"
	schemaTypeBoolean = "boolean"
	schemaTypeArray   = "array"
)

type Schema struct {
	// JSON schema type (for example: object, string, integer, boolean, array).
	Type string `json:"type,omitempty"`
	// Human-readable description shown to MCP clients/agents.
	Description string `json:"description,omitempty"`
	// Object field definitions keyed by property name.
	Properties map[string]*Schema `json:"properties,omitempty"`
	// Element schema for array-typed values.
	Items *Schema `json:"items,omitempty"`
	// Object property names that must be present.
	Required []string `json:"required,omitempty"`
	// Whether unknown object properties are allowed.
	AdditionalProperties *bool `json:"additionalProperties,omitempty"`
	// Inclusive minimum allowed numeric value.
	Minimum *int `json:"minimum,omitempty"`
	// Inclusive maximum allowed numeric value.
	Maximum *int `json:"maximum,omitempty"`
	// Minimum number of elements allowed in an array.
	MinItems *int `json:"minItems,omitempty"`
}

// StrictObjectSchema returns an object schema with additionalProperties=false.
// Required fields are provided via the separate required list because JSON
// Schema models object requiredness at the object level ("required"), not per
// property. Keeping this shape preserves MCP/JSON Schema compatibility.
func StrictObjectSchema(properties map[string]*Schema, required ...string) *Schema {
	return &Schema{
		Type:                 schemaTypeObject,
		Properties:           properties,
		Required:             append([]string{}, required...),
		AdditionalProperties: new(false),
	}
}

func StringSchema(description string) *Schema {
	return &Schema{
		Type:        schemaTypeString,
		Description: description,
	}
}

func IntegerSchema(description string, minimum, maximum *int) *Schema {
	return &Schema{
		Type:        schemaTypeInteger,
		Description: description,
		Minimum:     minimum,
		Maximum:     maximum,
	}
}

func BooleanSchema(description string) *Schema {
	return &Schema{
		Type:        schemaTypeBoolean,
		Description: description,
	}
}

func ArraySchema(items *Schema, description string, minItems *int) *Schema {
	return &Schema{
		Type:        schemaTypeArray,
		Description: description,
		Items:       items,
		MinItems:    minItems,
	}
}

type Tool struct {
	MCPTool *mcpsdk.Tool
	Timeout time.Duration
	Handler func(context.Context, map[string]any) (any, error)
}

type Service struct {
	target string
	conn   *grpc_client.ClientConnPool

	clientsMu sync.Mutex
	bbClient  bbspb.BuildBuddyServiceClient
	bsClient  bspb.ByteStreamClient
	tools     map[string]*Tool

	timingProfileResourceNameCacheMu sync.RWMutex
	timingProfileResourceNameCache   map[string]timingProfileResourceNameCacheEntry
}

func New(target string) *Service {
	s := &Service{
		target:                         target,
		timingProfileResourceNameCache: make(map[string]timingProfileResourceNameCacheEntry),
	}
	s.tools = map[string]*Tool{
		// Derived from local repo CI-file discovery + GitHub REST branch protection
		// endpoints (queried via `gh api`), not a BuildBuddy RPC.
		"ci_setup_info": {
			MCPTool: &mcpsdk.Tool{
				Name:        "ci_setup_info",
				Description: "Detect CI setup for the current repo and suggest which MCP tools to use next. Also does a best-effort GitHub branch-protection lookup (via gh API) to report required CI checks.",
				InputSchema: StrictObjectSchema(map[string]*Schema{}),
			},
			Timeout: 10 * time.Second,
			Handler: s.ciSetupInfo,
		},
		// Derived from GetInvocationRequest in proto/invocation.proto via
		// BuildBuddyService.GetInvocation in proto/buildbuddy_service.proto.
		"get_invocation": {
			MCPTool: &mcpsdk.Tool{
				Name:        "get_invocation",
				Description: "Get BuildBuddy invocation metadata for an invocation ID or invocation URL.",
				InputSchema: StrictObjectSchema(
					map[string]*Schema{
						"invocation_id": StringSchema("Invocation ID or BuildBuddy invocation URL."),
					},
					"invocation_id",
				),
			},
			Timeout: 30 * time.Second,
			Handler: s.getInvocation,
		},
		// Derived from /file/download HTTP endpoint in
		// server/buildbuddy_server/buildbuddy_server.go (ServeHTTP/serveBytestream).
		"get_invocation_artifact": {
			MCPTool: &mcpsdk.Tool{
				Name:        "get_invocation_artifact",
				Description: "Download an invocation bytestream:// artifact via BuildBuddy's /file/download endpoint to a local output path.",
				InputSchema: StrictObjectSchema(
					map[string]*Schema{
						"invocation_id":  StringSchema("Invocation ID or BuildBuddy invocation URL."),
						"bytestream_url": StringSchema("Bytestream URI of the artifact to download."),
						"output_path":    StringSchema("Local file path where artifact contents should be written."),
					},
					"invocation_id",
					"bytestream_url",
					"output_path",
				),
			},
			Timeout: 2 * time.Minute,
			Handler: s.getInvocationArtifact,
		},
		// Derived from SearchInvocationRequest in proto/invocation.proto via
		// BuildBuddyService.SearchInvocation in proto/buildbuddy_service.proto.
		"search_invocations": {
			MCPTool: &mcpsdk.Tool{
				Name:        "search_invocations",
				Description: "Search recent BuildBuddy invocations (bazel builds/tests or workflows).",
				InputSchema: StrictObjectSchema(
					map[string]*Schema{
						"user":                 StringSchema("Optional invocation user filter. Defaults to current Linux username unless all_users is true."),
						"all_users":            BooleanSchema("If true and user is unset, do not apply default current-user filtering."),
						"repo_url":             StringSchema("Optional repo URL filter."),
						"branch_name":          StringSchema("Optional branch filter."),
						"command":              StringSchema("Optional Bazel command filter (for example: build, test, run)."),
						"pattern":              StringSchema("Optional target pattern filter (exact match), for example: //..."),
						"workflow_action_name": StringSchema("Alias for pattern. Cannot be set together with pattern."),
						"role":                 StringSchema("Optional role filter (for example: CI)."),
						"lookback_days":        IntegerSchema("Lookback window in days when updated_after is unset. Applied only when this field is provided.", new(1), new(maxLookbackDays)),
						"updated_after":        StringSchema("Optional lower bound for updated time (RFC3339 or YYYY-MM-DD)."),
						"updated_before":       StringSchema("Optional upper bound for updated time (RFC3339 or YYYY-MM-DD)."),
						"sort_field":           StringSchema("Optional sort field (for example: updated_at_usec, duration, created_at_usec)."),
						"ascending":            BooleanSchema("Sort ascending when true. Default: false."),
						"count":                IntegerSchema("Optional number of invocations to return (max 100). Use page_token to fetch more results.", new(1), new(maxSearchCount)),
						"page_token":           StringSchema("Optional pagination token from a previous search_invocations call."),
					},
				),
			},
			Timeout: 45 * time.Second,
			Handler: s.searchInvocations,
		},
		// Derived from GetTargetRequest in proto/target.proto via
		// BuildBuddyService.GetTarget in proto/buildbuddy_service.proto.
		"get_target": {
			MCPTool: &mcpsdk.Tool{
				Name:        "get_target",
				Description: "Get invocation targets and target artifacts listing using BuildBuddy's GetTarget API.",
				InputSchema: StrictObjectSchema(
					map[string]*Schema{
						"invocation_id": StringSchema("Invocation ID or BuildBuddy invocation URL."),
						"target_label":  StringSchema("Optional target label to fetch (for example: //app:target)."),
						"status":        StringSchema("Optional target status (for example: PASSED, FAILED_TO_BUILD, FLAKY, 0)."),
						"filter":        StringSchema("Optional case-insensitive substring filter for labels/files."),
						"page_token":    StringSchema("Optional pagination token from a previous get_target call."),
					},
					"invocation_id",
				),
			},
			Timeout: 45 * time.Second,
			Handler: s.getTarget,
		},
		// Derived from GetExecutionRequest in proto/execution_stats.proto via
		// BuildBuddyService.GetExecution in proto/buildbuddy_service.proto.
		"get_executions": {
			MCPTool: &mcpsdk.Tool{
				Name:        "get_executions",
				Description: "List executions for an invocation using BuildBuddy's GetExecution API (does not inline ExecuteResponse).",
				InputSchema: StrictObjectSchema(
					map[string]*Schema{
						"invocation_id": StringSchema("Invocation ID or BuildBuddy invocation URL."),
						"target_label":  StringSchema("Optional target label filter (exact match)."),
						"action_digest": StringSchema("Optional action digest hash filter."),
					},
					"invocation_id",
				),
			},
			Timeout: 45 * time.Second,
			Handler: s.getExecutions,
		},
		// Derived from GetExecutionRequest in proto/execution_stats.proto via
		// BuildBuddyService.GetExecution in proto/buildbuddy_service.proto.
		"get_execution": {
			MCPTool: &mcpsdk.Tool{
				Name:        "get_execution",
				Description: "Get full metadata for a single execution by execution ID, including inlined ExecuteResponse when available.",
				InputSchema: StrictObjectSchema(
					map[string]*Schema{
						"execution_id":  StringSchema("Execution ID to fetch."),
						"invocation_id": StringSchema("Optional invocation ID or URL to scope lookup."),
					},
					"execution_id",
				),
			},
			Timeout: 45 * time.Second,
			Handler: s.getExecution,
		},
		// Derived from a composite workflow using GetInvocationRequest (proto/invocation.proto),
		// FetchBlobRequest (proto/remote_asset.proto), and RE/CAS protos in
		// proto/remote_execution.proto.
		"analyze_profile": {
			MCPTool: &mcpsdk.Tool{
				Name:        "analyze_profile",
				Description: "Diagnose slow Bazel/CI builds by analyzing timing profiles for one or more invocations. Returns compact metrics by default (plus insights/diagnostics), and can optionally include detailed summary sections. Prefer passing multiple invocation_ids so analyses run in parallel and aggregate recurring bottlenecks.",
				InputSchema: StrictObjectSchema(
					map[string]*Schema{
						"invocation_ids": ArraySchema(
							StringSchema(""),
							"Invocation IDs or invocation URLs to analyze. Strongly encouraged to pass multiple IDs so analysis can run in parallel.",
							new(1),
						),
						"invocation_id":                  StringSchema("Convenience alias for analyzing a single invocation."),
						"top_n":                          IntegerSchema("How many top entries to return for spans/categories/threads. Default: 10.", new(1), new(maxTopN)),
						"parallelism":                    IntegerSchema("Maximum number of invocations to analyze concurrently during remote execution. Default: 100.", new(1), new(maxREParallelism)),
						"hints":                          BooleanSchema("If true, include actionable follow-up hints (for example suggested get_executions calls for slow targets when --remote_executor is set)."),
						"include_summary":                BooleanSchema("If true, include per-invocation summary payloads in addition to compact metrics. Default: false."),
						"include_summary_top_spans":      BooleanSchema("When include_summary is enabled, include top_spans_by_duration_usec. Default: true when include_summary=true, otherwise false."),
						"include_summary_top_categories": BooleanSchema("When include_summary is enabled, include top_categories_by_duration_usec. Default: true when include_summary=true, otherwise false."),
						"include_summary_top_threads":    BooleanSchema("When include_summary is enabled, include top_threads_by_duration_usec. Default: false."),
						"include_summary_critical_path_components": BooleanSchema("When include_summary is enabled, include critical_path_components. Default: false."),
						"max_output_bytes":                         IntegerSchema("Maximum serialized JSON response size before truncating low-priority fields. Default: 65536, max: 10485760.", new(1), new(maxAnalyzeProfileMaxOutputBytes)),
						"extra_args": ArraySchema(
							StringSchema("Raw CLI argument passed through to bb-analyze-profile (for example: subcommand names or experimental flags)."),
							"Optional raw args passed to bb-analyze-profile before default --profile and --top_n flags.",
							nil,
						),
					},
				),
			},
			Timeout: 10 * time.Minute,
			Handler: s.analyzeProfile,
		},
		// Derived from local CLI help text in cli/analyze_profile (not an RPC).
		"analyze_profile_help": {
			MCPTool: &mcpsdk.Tool{
				Name:        "analyze_profile_help",
				Description: "Return the full embedded help text for bb analyze-profile plus the full Go source for the analyze_profile output types.",
				InputSchema: StrictObjectSchema(map[string]*Schema{}),
			},
			Timeout: 10 * time.Second,
			Handler: s.analyzeProfileHelp,
		},
	}
	return s
}

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "mcptools",
    srcs = [
        "mcptools.go",
        "tools_common.go",
        "tool_analyze_profile.go",
        "tool_ci_setup_info.go",
        "tool_get_invocation.go",
        "tool_get_invocation_artifact.go",
        "tool_get_executions.go",
        "tool_get_target.go",
        "tool_search_invocations.go",
    ],
    embedsrcs = ["//cli/mcp/mcptools/analyzeprofileschema:schema.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/mcp/mcptools",
    deps = [
        "//cli/analyze_profile",
        "//cli/login",
        "//cli/mcp/mcptools/analyzeprofileschema",
        "//cli/storage",
        "//cli/timing_profile",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:execution_stats_go_proto",
        "//proto:invocation_go_proto",
        "//proto:remote_asset_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:target_go_proto",
        "//proto/api/v1:common_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/lib/set",
        "//server/util/grpc_client",
        "//server/util/rexec",
        "//server/util/trace_events",
        "@com_github_modelcontextprotocol_go_sdk//mcp",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//encoding/protojson",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "mcptools_test",
    srcs = [
        "mcptools_test.go",
        "tool_analyze_profile_test.go",
    ],
    embed = [":mcptools"],
)

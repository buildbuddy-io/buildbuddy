load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "analyze_profile",
    srcs = ["analyze_profile.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/agent/analyze_profile",
    deps = [
        "//cli/agent/agentflags",
        "//cli/agent/analyze_profile/ztracing",
        "//cli/log",
        "//cli/terminal",
        "//cli/util/agent",
        "//cli/util/agent/agentutil",
        "//cli/util/download",
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//server/util/grpc_client",
        "//server/util/uuid",
    ],
)

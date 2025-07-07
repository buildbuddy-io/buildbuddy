load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tools",
    srcs = ["tools.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/tools",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:target_go_proto",
        "//proto/api/v1:api_v1_go_proto",
        "//server/environment",
        "//server/util/log",
        "//server/util/rexec",
        "//server/util/shlex",
        "//server/util/status",
        "@com_github_modelcontextprotocol_go_sdk//mcp",
        "@com_github_psanford_memfs//:memfs",
        "@org_golang_google_protobuf//types/known/durationpb",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

package(default_visibility = ["//experimental:__subpackages__"])

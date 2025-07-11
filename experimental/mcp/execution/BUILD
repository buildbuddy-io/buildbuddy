load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "execution",
    srcs = ["execution.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/execution",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/util/log",
        "//server/util/rexec",
        "//server/util/shlex",
        "//server/util/status",
        "@com_github_modelcontextprotocol_go_sdk//mcp",
        "@org_golang_google_protobuf//types/known/durationpb",
    ],
)

package(default_visibility = ["//experimental:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "ask",
    srcs = ["ask.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/ask",
    deps = [
        "//cli/arg",
        "//cli/artifacts",
        "//cli/flaghistory",
        "//cli/log",
        "//cli/login",
        "//cli/terminal",
        "//cli/util/agent",
        "//cli/util/agent/agentutil",
        "//proto:buildbuddy_service_go_proto",
        "//proto:suggestion_go_proto",
        "//server/util/flagutil/types",
        "//server/util/git",
        "//server/util/grpc_client",
        "//server/util/shlex",
        "@org_golang_google_grpc//metadata",
    ],
)

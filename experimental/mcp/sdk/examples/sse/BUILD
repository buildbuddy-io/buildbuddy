load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "sse_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/sdk/examples/sse",
    visibility = ["//visibility:private"],
    deps = ["//experimental/mcp/sdk"],
)

go_binary(
    name = "sse",
    embed = [":sse_lib"],
    visibility = ["//visibility:public"],
)

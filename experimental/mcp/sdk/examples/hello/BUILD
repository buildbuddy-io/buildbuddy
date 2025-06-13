load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "hello_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/sdk/examples/hello",
    visibility = ["//visibility:private"],
    deps = ["//experimental/mcp/sdk"],
)

go_binary(
    name = "hello",
    embed = [":hello_lib"],
    visibility = ["//visibility:public"],
)

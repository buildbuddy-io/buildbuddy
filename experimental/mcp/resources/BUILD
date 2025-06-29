load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "resources",
    srcs = ["resources.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/resources",
    visibility = ["//visibility:public"],
    deps = [
        "//experimental/modelcontextprotocol/go-sdk/mcp",
        "//server/environment",
    ],
)

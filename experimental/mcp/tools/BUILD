load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tools",
    srcs = ["tools.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/tools",
    visibility = ["//visibility:public"],
    deps = [
        "//experimental/modelcontextprotocol/go-sdk/mcp",
        "//proto/api/v1:api_v1_go_proto",
        "//server/environment",
        "//server/util/log",
        "//server/util/status",
    ],
)

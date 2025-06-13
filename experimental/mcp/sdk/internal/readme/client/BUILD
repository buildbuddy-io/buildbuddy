load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "client_lib",
    srcs = ["client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/sdk/internal/readme/client",
    visibility = ["//visibility:private"],
    deps = ["//experimental/mcp/sdk"],
)

go_binary(
    name = "client",
    embed = [":client_lib"],
    visibility = ["//experimental/mcp/sdk/mcp:__subpackages__"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "server_lib",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/modelcontextprotocol/go-sdk/internal/readme/server",
    visibility = ["//visibility:private"],
    deps = ["//experimental/modelcontextprotocol/go-sdk/mcp"],
)

go_binary(
    name = "server",
    embed = [":server_lib"],
    visibility = ["//experimental/modelcontextprotocol/go-sdk:__subpackages__"],
)

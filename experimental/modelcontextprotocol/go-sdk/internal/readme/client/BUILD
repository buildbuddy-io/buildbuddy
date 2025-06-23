load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "client_lib",
    srcs = ["client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/modelcontextprotocol/go-sdk/internal/readme/client",
    visibility = ["//visibility:private"],
    deps = ["//experimental/modelcontextprotocol/go-sdk/mcp"],
)

go_binary(
    name = "client",
    embed = [":client_lib"],
    visibility = ["//experimental/modelcontextprotocol/go-sdk:__subpackages__"],
)

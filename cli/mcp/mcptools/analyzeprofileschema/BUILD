load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "analyzeprofileschema",
    srcs = ["schema.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/mcp/mcptools/analyzeprofileschema",
    deps = ["//server/util/trace_events"],
)

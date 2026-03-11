load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "mcp",
    srcs = ["mcp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/mcp",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/mcp/mcptools",
        "//cli/version",
        "@com_github_modelcontextprotocol_go_sdk//mcp",
    ],
)

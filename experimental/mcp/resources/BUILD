load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "resources",
    srcs = ["resources.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/resources",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "@com_github_modelcontextprotocol_go_sdk//mcp",
    ],
)

package(default_visibility = ["//experimental:__subpackages__"])

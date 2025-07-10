load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "schema",
    srcs = ["schema.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/experimental/mcp/schema",
    visibility = ["//visibility:public"],
    deps = ["@com_github_modelcontextprotocol_go_sdk//jsonschema"],
)

package(default_visibility = ["//experimental:__subpackages__"])

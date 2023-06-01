load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "suggestion",
    srcs = ["suggestion.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/suggestion",
    deps = [
        "//enterprise/server/backends/openai",
        "//enterprise/server/backends/vertexai",
        "//proto:eventlog_go_proto",
        "//proto:suggestion_go_proto",
        "//server/environment",
        "//server/eventlog",
        "//server/util/log",
        "//server/util/status",
    ],
)

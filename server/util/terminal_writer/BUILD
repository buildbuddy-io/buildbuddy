load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "terminal_writer",
    srcs = ["terminal_writer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/terminal_writer",
    visibility = ["//visibility:public"],
    deps = ["@com_github_buildkite_terminal_to_html_v3//:terminal-to-html"],
)

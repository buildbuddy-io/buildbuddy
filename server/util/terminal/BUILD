load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "terminal",
    srcs = ["terminal.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/terminal",
    visibility = ["//visibility:public"],
    deps = ["@com_github_buildkite_terminal_to_html_v3//:terminal-to-html"],
)

go_test(
    name = "terminal_test",
    srcs = ["terminal_test.go"],
    deps = [
        ":terminal",
        "//server/util/random",
    ],
)

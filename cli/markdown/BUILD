load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "markdown",
    srcs = ["markdown.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/markdown",
    deps = ["//cli/terminal"],
)

go_test(
    name = "markdown_test",
    srcs = ["markdown_test.go"],
    embed = [":markdown"],
)

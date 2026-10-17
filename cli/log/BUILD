load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "log",
    srcs = ["log.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/log",
    deps = ["//cli/terminal"],
)

go_test(
    name = "log_test",
    srcs = ["log_test.go"],
    embed = [":log"],
    deps = ["@com_github_stretchr_testify//require"],
)

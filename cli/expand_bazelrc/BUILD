load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "expand_bazelrc",
    srcs = ["expand_bazelrc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/expand_bazelrc",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/parser",
    ],
)

go_test(
    name = "expand_bazelrc_test",
    srcs = ["expand_bazelrc_test.go"],
    embed = [":expand_bazelrc"],
    deps = [
        "@com_github_stretchr_testify//require",
    ],
)

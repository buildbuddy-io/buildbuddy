load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "setup",
    srcs = [
        "late_bazelrc.go",
        "setup.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/setup",
    deps = [
        "//cli/arg",
        "//cli/flaghistory",
        "//cli/login",
        "//cli/log",
        "//cli/parser",
        "//cli/parser/options",
        "//cli/parser/parsed",
        "//cli/plugin",
        "//cli/sidecar",
        "//cli/tooltag",
    ],
)

go_test(
    name = "setup_test",
    srcs = ["late_bazelrc_test.go"],
    embed = [":setup"],
    deps = [
        "//cli/log",
        "//cli/parser",
        "//cli/parser/test_data",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

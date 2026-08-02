load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bbrc",
    srcs = ["bbrc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/bbrc",
    deps = [
        "//cli/cli_command",
        "//cli/log",
        "//cli/parser/bazelrc",
        "//cli/workspace",
    ],
)

go_test(
    name = "bbrc_test",
    srcs = ["bbrc_test.go"],
    embed = [":bbrc"],
    deps = [
        "//cli/cli_command",
        "//cli/cli_command/register",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "help",
    srcs = ["help.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/help",
    deps = [
        "//cli/bazelisk",
        "//cli/cli_command",
        "//cli/parser/arguments",
        "//cli/parser/parsed",
        "//cli/version",
    ],
)

go_test(
    name = "help_test",
    srcs = ["help_test.go"],
    embed = [":help"],
    deps = [
        "//cli/cli_command",
        "//cli/parser/arguments",
        "//cli/parser/parsed",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

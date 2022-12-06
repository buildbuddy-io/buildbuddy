load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "plugin",
    srcs = ["plugin.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/plugin",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/log",
        "//cli/parser",
        "//cli/storage",
        "//cli/terminal",
        "//cli/workspace",
        "//server/util/disk",
        "//server/util/git",
        "//server/util/status",
        "@com_github_creack_pty//:pty",
        "@in_gopkg_yaml_v2//:yaml_v2",
    ],
)

go_test(
    name = "plugin_test",
    srcs = ["plugin_test.go"],
    embed = [":plugin"],
    deps = [
        "//cli/log",
        "//cli/workspace",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

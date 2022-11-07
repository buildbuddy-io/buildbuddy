load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "parser",
    srcs = ["parser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/workspace",
    ],
)

go_test(
    name = "parser_test",
    srcs = ["parser_test.go"],
    embed = [":parser"],
    deps = [
        "//cli/log",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

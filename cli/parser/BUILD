load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "parser",
    srcs = ["parser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/log",
        "//cli/storage",
        "//cli/workspace",
        "//server/remote_cache/digest",
        "//server/util/disk",
        "@com_github_google_shlex//:shlex",
    ],
)

go_test(
    name = "parser_test",
    srcs = ["parser_test.go"],
    embed = [":parser"],
    deps = [
        "//cli/log",
        "//cli/parser/test_data",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

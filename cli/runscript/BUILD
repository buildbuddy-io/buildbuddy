load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "runscript",
    srcs = ["runscript.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/runscript",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/stream_run_logs",
    ],
)

go_test(
    name = "runscript_test",
    srcs = ["runscript_test.go"],
    embed = [":runscript"],
    deps = [
        "//cli/arg",
        "//cli/parser",
        "//cli/parser/test_data",
        "//cli/workspace",
        "@com_github_stretchr_testify//require",
    ],
)

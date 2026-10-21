load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "update",
    srcs = ["update.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/update",
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/log",
        "//cli/workspace",
    ],
)

go_test(
    name = "update_test",
    size = "small",
    srcs = ["update_test.go"],
    embed = [":update"],
    deps = ["@com_github_stretchr_testify//require"],
)

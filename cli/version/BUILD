load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "version",
    srcs = ["version.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/version",
    deps = [
        "//cli/arg",
        "//cli/bazel_command",
        "//cli/common",
    ],
)

go_test(
    name = "version_test",
    srcs = ["version_test.go"],
    deps = [
        ":version",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

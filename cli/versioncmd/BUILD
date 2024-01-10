load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "versioncmd",
    srcs = ["versioncmd.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/versioncmd",
    deps = [
        "//cli/arg",
        "//cli/plugin",
        "//cli/setup",
        "//cli/version",
        "//server/util/status",
    ],
)

go_test(
    name = "versioncmd_test",
    srcs = ["versioncmd_test.go"],
    deps = [
        ":versioncmd",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

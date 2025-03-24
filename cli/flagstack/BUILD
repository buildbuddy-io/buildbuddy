load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "flagstack",
    srcs = ["flagstack.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/flagstack",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/workspace",
        "//server/util/hash",
        "//server/util/uuid",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

go_test(
    name = "flagstack_test",
    srcs = ["flagstack_test.go"],
    embed = [":flagstack"],
    deps = ["@com_github_stretchr_testify//require"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bzlmod",
    srcs = ["bzlmod.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/bzlmod",
    deps = ["@com_github_bazel_contrib_buildtools_v10//build"],
)

go_test(
    name = "bzlmod_test",
    size = "small",
    srcs = ["bzlmod_test.go"],
    deps = [
        ":bzlmod",
        "//server/testutil/testfs",
        "@com_github_stretchr_testify//require",
    ],
)

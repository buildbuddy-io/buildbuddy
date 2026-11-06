load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "git",
    srcs = ["git.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/util/git",
    deps = ["//server/util/redact"],
)

go_test(
    name = "git_test",
    srcs = ["git_test.go"],
    deps = [
        ":git",
        "@com_github_stretchr_testify//require",
    ],
)

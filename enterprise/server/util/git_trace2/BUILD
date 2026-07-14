load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "git_trace2",
    srcs = ["git_trace2.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/git_trace2",
)

go_test(
    name = "git_trace2_test",
    size = "small",
    srcs = ["git_trace2_test.go"],
    deps = [
        ":git_trace2",
        "@com_github_stretchr_testify//require",
    ],
)

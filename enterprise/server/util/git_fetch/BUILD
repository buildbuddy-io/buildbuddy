load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "git_fetch",
    srcs = ["git_fetch.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/git_fetch",
    deps = [
        "//enterprise/server/util/git_trace2",
        "//server/util/log",
        "@com_github_docker_go_units//:go-units",
    ],
)

go_test(
    name = "git_fetch_test",
    size = "small",
    srcs = ["git_fetch_test.go"],
    deps = [
        ":git_fetch",
        "//server/testutil/testgit",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//require",
    ],
)

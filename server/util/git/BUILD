load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "git",
    srcs = ["git.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/git",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "git_test",
    size = "small",
    srcs = ["git_test.go"],
    deps = [
        ":git",
        "@com_github_stretchr_testify//assert",
    ],
)

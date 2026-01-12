load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "gitserver",
    srcs = ["gitserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/gitserver",
    visibility = ["//visibility:public"],
    deps = ["//server/util/log"],
)

go_test(
    name = "gitserver_test",
    srcs = ["gitserver_test.go"],
    deps = [
        ":gitserver",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

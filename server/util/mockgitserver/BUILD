load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "mockgitserver",
    srcs = ["mockgitserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/mockgitserver",
    visibility = ["//visibility:public"],
    deps = ["//server/util/log"],
)

go_test(
    name = "mockgitserver_test",
    srcs = ["mockgitserver_test.go"],
    deps = [
        ":mockgitserver",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

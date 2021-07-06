load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testgit",
    testonly = 1,
    srcs = ["testgit.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testgit",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/testutil/testfs",
        "//server/testutil/testshell",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)

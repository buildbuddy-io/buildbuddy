load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testhttp",
    testonly = 1,
    srcs = ["testhttp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp",
    visibility = ["//visibility:public"],
    deps = [
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_stretchr_testify//require",
    ],
)

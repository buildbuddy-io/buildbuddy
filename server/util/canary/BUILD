load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "canary",
    srcs = ["canary.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/canary",
    visibility = ["//visibility:public"],
    deps = ["//server/util/log"],
)

go_test(
    name = "canary_test",
    size = "small",
    srcs = ["canary_test.go"],
    deps = [
        ":canary",
        "@com_github_stretchr_testify//require",
    ],
)

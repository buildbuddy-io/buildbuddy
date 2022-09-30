load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "retry",
    srcs = ["retry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/retry",
    visibility = ["//visibility:public"],
)

go_test(
    name = "retry_test",
    size = "small",
    srcs = ["retry_test.go"],
    deps = [
        ":retry",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

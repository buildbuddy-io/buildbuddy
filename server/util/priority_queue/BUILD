load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "priority_queue",
    srcs = ["priority_queue.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/priority_queue",
    visibility = ["//visibility:public"],
)

go_test(
    name = "priority_queue_test",
    srcs = ["priority_queue_test.go"],
    deps = [
        ":priority_queue",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

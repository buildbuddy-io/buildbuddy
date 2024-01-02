load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "boundedstack",
    srcs = ["boundedstack.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/boundedstack",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
    ],
)

go_test(
    name = "boundedstack_test",
    srcs = ["boundedstack_test.go"],
    embed = [":boundedstack"],
    deps = [
        "@com_github_stretchr_testify//require",
    ],
)

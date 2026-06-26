load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "slack",
    srcs = ["slack.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/slack",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:invocation_go_proto",
        "//server/endpoint_urls/build_buddy_url",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/status",
    ],
)

go_test(
    name = "slack_test",
    srcs = ["slack_test.go"],
    embed = [":slack"],
    deps = [
        "//server/interfaces",
        "//server/real_environment",
        "//server/testutil/testauth",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "invocation_format",
    srcs = ["invocation_format.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/invocation_format",
    visibility = ["//visibility:public"],
)

go_test(
    name = "invocation_format_test",
    size = "small",
    srcs = ["invocation_format_test.go"],
    deps = [
        ":invocation_format",
        "@com_github_stretchr_testify//assert",
    ],
)

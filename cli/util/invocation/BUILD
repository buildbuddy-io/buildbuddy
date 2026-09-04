load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "invocation",
    srcs = ["invocation.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/util/invocation",
    deps = [
        "//cli/log",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
        "//proto:option_filters_go_proto",
    ],
)

go_test(
    name = "invocation_test",
    srcs = ["invocation_test.go"],
    embed = [":invocation"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
        "//proto:option_filters_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["event_parser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
        "//server/terminal:go_default_library",
        "//server/util/git:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["event_parser_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

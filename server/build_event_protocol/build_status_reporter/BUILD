load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["build_status_reporter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_status_reporter",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//server/backends/github:go_default_library",
        "//server/build_event_protocol/accumulator:go_default_library",
        "//server/build_event_protocol/event_parser:go_default_library",
        "//server/environment:go_default_library",
    ],
)

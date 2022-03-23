load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "build_status_reporter",
    srcs = ["build_status_reporter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/build_status_reporter",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//server/backends/github",
        "//server/build_event_protocol/accumulator",
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/tables",
        "//server/util/git",
        "//server/util/log",
        "//server/util/timeutil",
    ],
)

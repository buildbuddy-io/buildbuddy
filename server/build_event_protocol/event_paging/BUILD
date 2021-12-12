load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "event_paging",
    srcs = ["event_paging.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_paging",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:invocation_go_proto",
    ],
)

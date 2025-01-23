load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "buck",
    srcs = ["buck.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/buck",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:buck_data_go_proto",
        "//proto:build_event_stream_go_proto",
        "//proto:build_events_go_proto",
    ],
)

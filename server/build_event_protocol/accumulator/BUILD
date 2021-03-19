load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["accumulator.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/accumulator",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//server/util/git:go_default_library",
    ],
)

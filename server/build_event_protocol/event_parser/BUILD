load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["event_parser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/build_event_protocol/event_parser",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:command_line_go_proto",
        "//proto:invocation_go_proto",
        "@org_golang_x_crypto//ssh/terminal:go_default_library",
    ],
)

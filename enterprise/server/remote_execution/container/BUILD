load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "container",
    srcs = ["container.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/util/tracing",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel_trace//:trace",
    ],
)

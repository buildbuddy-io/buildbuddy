load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "span",
    srcs = ["span.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/tracing/span",
    visibility = ["//visibility:public"],
    deps = [
        "@io_opentelemetry_go_otel//:otel",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel_trace//:trace",
    ],
)

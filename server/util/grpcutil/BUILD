load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "grpcutil",
    srcs = ["grpcutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/grpcutil",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/alert",
        "@io_opentelemetry_go_otel_exporters_prometheus//:prometheus",
        "@io_opentelemetry_go_otel_metric//:metric",
        "@io_opentelemetry_go_otel_metric//noop",
        "@io_opentelemetry_go_otel_sdk_metric//:metric",
    ],
)

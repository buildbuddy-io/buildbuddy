load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tracing",
    srcs = ["tracing.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/tracing",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:trace_go_proto",
        "//server/config",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@com_github_googlecloudplatform_opentelemetry_operations_go_exporter_trace//:trace",
        "@io_opentelemetry_go_otel//:otel",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel//propagation",
        "@io_opentelemetry_go_otel//semconv",
        "@io_opentelemetry_go_otel_sdk//resource",
        "@io_opentelemetry_go_otel_sdk//trace",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_grpc//metadata",
    ],
)

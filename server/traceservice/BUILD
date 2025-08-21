load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "traceservice",
    srcs = ["traceservice.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/traceservice",
    visibility = ["//visibility:public"],
    deps = [
        "//server/real_environment",
        "//server/util/log",
        "//server/util/status",
        "@io_opentelemetry_go_otel_exporters_otlp_otlptrace//:otlptrace",
        "@io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc//:otlptracegrpc",
        "@io_opentelemetry_go_proto_otlp//collector/trace/v1:trace",
    ],
)

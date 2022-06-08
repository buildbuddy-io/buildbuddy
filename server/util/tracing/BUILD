load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tracing",
    srcs = ["tracing.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/tracing",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:trace_go_proto",
        "//server/environment",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/status",
        "@io_opentelemetry_go_contrib_detectors_gcp//:gcp",
        "@io_opentelemetry_go_contrib_instrumentation_net_http_otelhttp//:otelhttp",
        "@io_opentelemetry_go_otel//:otel",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel//propagation",
        "@io_opentelemetry_go_otel//semconv/v1.4.0:v1_4_0",
        "@io_opentelemetry_go_otel_exporters_jaeger//:jaeger",
        "@io_opentelemetry_go_otel_sdk//resource",
        "@io_opentelemetry_go_otel_sdk//trace",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_grpc//metadata",
    ],
)

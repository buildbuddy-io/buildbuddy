load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "otel-exporter_lib",
    srcs = [
        "bb.go",
        "cache.go",
        "deamon.go",
        "main.go",
        "otel.go",
        "profile.go",
        "tef.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/otel-exporter",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:context_go_proto",
        "//proto:invocation_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel//codes",
        "@io_opentelemetry_go_otel//semconv/v1.11.0:v1_11_0",
        "@io_opentelemetry_go_otel_exporters_otlp_otlptrace//:otlptrace",
        "@io_opentelemetry_go_otel_exporters_otlp_otlptrace_otlptracegrpc//:otlptracegrpc",
        "@io_opentelemetry_go_otel_sdk//resource",
        "@io_opentelemetry_go_otel_sdk//trace",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "otel-exporter",
    embed = [":otel-exporter_lib"],
    visibility = ["//visibility:public"],
)

go_binary(
    name = "otel-exporter-linux-arm",
    embed = [":otel-exporter_lib"],
    goarch = "arm",
    goos = "linux",
    tags = ["manual"],
    visibility = ["//visibility:public"],
)

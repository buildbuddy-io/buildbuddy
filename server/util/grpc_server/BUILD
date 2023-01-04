load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "grpc_server",
    srcs = ["grpc_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/grpc_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:health_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/rpc/interceptors",
        "//server/util/bazel_request",
        "//server/util/log",
        "//server/util/status",
        "@com_github_grpc_ecosystem_go_grpc_prometheus//:go-grpc-prometheus",
        "@io_opentelemetry_go_contrib_instrumentation_google_golang_org_grpc_otelgrpc//:otelgrpc",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//encoding/gzip",
        "@org_golang_google_grpc//keepalive",
        "@org_golang_google_grpc//reflection",
    ],
)

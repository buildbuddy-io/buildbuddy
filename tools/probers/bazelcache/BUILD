load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "bazelcache_lib",
    srcs = ["bazelcache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/probers/bazelcache",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/real_environment",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/bazel_request",
        "//server/util/compression",
        "//server/util/grpc_client",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/tracing",
        "//server/util/uuid",
        "@io_opentelemetry_go_otel//:otel",
        "@io_opentelemetry_go_otel//attribute",
        "@io_opentelemetry_go_otel//codes",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//stats",
        "@org_golang_google_grpc//status",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "bazelcache",
    embed = [":bazelcache_lib"],
    visibility = ["//visibility:public"],
)

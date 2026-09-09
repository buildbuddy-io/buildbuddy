load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

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

go_test(
    name = "bazelcache_test",
    srcs = ["bazelcache_test.go"],
    embed = [":bazelcache_lib"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/bazel_request",
        "//server/util/grpc_client",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
        "@io_opentelemetry_go_otel//:otel",
        "@io_opentelemetry_go_otel_sdk//trace",
        "@io_opentelemetry_go_otel_trace//:trace",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//status",
    ],
)

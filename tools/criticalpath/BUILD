load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "criticalpath_lib",
    srcs = ["criticalpath.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/criticalpath",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:build_event_stream_go_proto",
        "//proto:buildbuddy_service_go_proto",
        "//proto:invocation_go_proto",
        "//server/backends/blobstore/gcs",
        "//server/interfaces",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/compression",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/trace_events",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "criticalpath",
    embed = [":criticalpath_lib"],
    visibility = ["//visibility:public"],
)

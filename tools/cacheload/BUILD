load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "cacheload_lib",
    srcs = ["cacheload.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/cacheload",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_time//rate",
    ],
)

go_binary(
    name = "cacheload",
    embed = [":cacheload_lib"],
    visibility = ["//visibility:public"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "cache_proxy",
    srcs = ["cache_proxy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/cache_proxy",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/byte_stream_server",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/content_addressable_storage_server",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//metadata",
        "@org_golang_google_grpc//test/bufconn",
        "@org_golang_google_protobuf//encoding/protojson",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "cachetools",
    srcs = ["cachetools.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/bytebufferpool",
        "//server/util/compression",
        "//server/util/log",
        "//server/util/status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//status",
        "@org_golang_google_protobuf//proto",
        "@org_golang_x_sync//errgroup",
    ],
)

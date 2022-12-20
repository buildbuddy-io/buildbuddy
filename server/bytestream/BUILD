load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bytestream",
    srcs = ["bytestream.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/bytestream",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:zip_go_proto",
        "//server/environment",
        "//server/remote_cache/digest",
        "//server/util/flagutil",
        "//server/util/grpc_client",
        "//server/util/status",
        "//server/util/ziputil",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_protobuf//proto",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bytestream",
    srcs = ["bytestream.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/bytestream",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/grpc_client",
        "//server/util/status",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)

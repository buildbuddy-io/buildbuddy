load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bytestream",
    srcs = ["bytestream.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/bytestream",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/status",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)

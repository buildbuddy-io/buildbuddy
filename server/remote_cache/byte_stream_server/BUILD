load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["byte_stream_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/util/perms:go_default_library",
        "//server/util/status:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)

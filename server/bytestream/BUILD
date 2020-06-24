load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["bytestream.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/bytestream",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment:go_default_library",
        "//server/util/grpc_client:go_default_library",
        "//server/util/status:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

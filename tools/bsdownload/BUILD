load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "bsdownload_lib",
    srcs = ["bsdownload.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bsdownload",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_binary(
    name = "bsdownload",
    embed = [":bsdownload_lib"],
    visibility = ["//visibility:public"],
)

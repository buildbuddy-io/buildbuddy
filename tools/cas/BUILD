load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "cas",
    srcs = ["cas.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/cas",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/grpc_client",
        "//server/util/log",
        "@com_github_golang_protobuf//proto:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "main",
    embed = [":cas"],
    visibility = ["//visibility:public"],
)

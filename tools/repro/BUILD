load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "repro_lib",
    srcs = ["repro.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/repro",
    visibility = ["//visibility:private"],
    deps = [
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/flagutil/types",
        "//server/util/grpc_client",
        "//server/util/log",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//metadata",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "repro",
    embed = [":repro_lib"],
    visibility = ["//visibility:public"],
)

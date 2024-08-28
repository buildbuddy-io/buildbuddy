load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "byte_stream_client",
    srcs = ["byte_stream_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_client",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/endpoint_urls/cache_api_url",
        "//server/environment",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/grpc_client",
        "//server/util/log",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/urlutil",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
    ],
)

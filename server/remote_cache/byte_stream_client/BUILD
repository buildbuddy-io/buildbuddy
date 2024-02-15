load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "byte_stream_client",
    srcs = ["byte_stream_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_client",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:zip_go_proto",
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
        "//server/util/ziputil",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:go_default_library",
    ],
)

go_test(
    name = "byte_stream_client_test",
    size = "small",
    srcs = ["byte_stream_client_test.go"],
    data = [
        "no_files.zip",
        "some_files.zip",
        "too_many_files.zip",
    ],
    embed = [":byte_stream_client"],
    deps = [
        "//proto:zip_go_proto",
        "//server/util/proto",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

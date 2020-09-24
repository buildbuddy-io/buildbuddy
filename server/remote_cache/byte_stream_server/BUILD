load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["byte_stream_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/remote_cache/hit_tracker:go_default_library",
        "//server/remote_cache/namespace:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/status:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
    ],
)

go_test(
    name = "go_default_test",
    size = "small",
    srcs = ["byte_stream_server_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest:go_default_library",
        "//server/test_environment:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/random:go_default_library",
        "//server/util/status:go_default_library",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_grpc//:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

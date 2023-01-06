load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "bytestream",
    srcs = ["bytestream.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/bytestream",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:zip_go_proto",
        "//server/environment",
        "//server/remote_cache/digest",
        "//server/util/flagutil",
        "//server/util/grpc_client",
        "//server/util/status",
        "//server/util/ziputil",
        "@go_googleapis//google/bytestream:bytestream_go_proto",
        "@org_golang_google_protobuf//proto",
    ],
)

go_test(
    name = "bytestream_test",
    size = "small",
    srcs = ["bytestream_test.go"],
    data = [
        "no_files.zip",
        "some_files.zip",
        "too_many_files.zip",
    ],
    embed = [":bytestream"],
    deps = [
        "//proto:zip_go_proto",
        "//server/environment",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
        "@org_golang_google_protobuf//proto",
    ],
)

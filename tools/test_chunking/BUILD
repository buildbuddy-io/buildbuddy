load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "test_chunking_lib",
    srcs = ["test_chunking_compression.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/test_chunking",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/compression",
        "@com_github_jotfs_fastcdc_go//:fastcdc-go",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_grpc//credentials/insecure",
    ],
)

go_binary(
    name = "test_chunking",
    embed = [":test_chunking_lib"],
    visibility = ["//visibility:public"],
)

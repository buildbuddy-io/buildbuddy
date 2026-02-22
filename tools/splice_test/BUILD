load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "splice_test_lib",
    srcs = ["main.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/splice_test",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/status",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//credentials",
        "@org_golang_google_grpc//credentials/insecure",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "splice_test",
    embed = [":splice_test_lib"],
    visibility = ["//visibility:public"],
)

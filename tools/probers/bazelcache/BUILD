load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "bazelcache_lib",
    srcs = ["bazelcache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/probers/bazelcache",
    visibility = ["//visibility:private"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/cachetools",
        "//server/remote_cache/digest",
        "//server/util/compression",
        "//server/util/grpc_client",
        "//server/util/log",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_bytestream//:bytestream",
        "@org_golang_google_grpc//:grpc",
        "@org_golang_google_grpc//metadata",
    ],
)

go_binary(
    name = "bazelcache",
    embed = [":bazelcache_lib"],
    visibility = ["//visibility:public"],
)

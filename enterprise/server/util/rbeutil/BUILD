load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "rbeutil",
    srcs = ["rbeutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/rbeutil",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/backends/redis_client",
        "//enterprise/server/remote_execution/config",
        "//enterprise/server/util/redisutil",
        "//proto:remote_execution_go_proto",
        "//server/real_environment",
        "//server/util/grpc_client",
        "//server/util/status",
    ],
)

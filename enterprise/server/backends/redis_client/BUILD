load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "redis_client",
    srcs = ["redis_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_client",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/remote_execution/config",
        "//enterprise/server/util/redisutil",
        "//server/environment",
        "//server/util/flagutil",
        "//server/util/status",
    ],
)

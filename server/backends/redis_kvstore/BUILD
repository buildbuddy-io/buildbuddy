load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "redis_kvstore",
    srcs = ["redis_kvstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/redis_kvstore",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

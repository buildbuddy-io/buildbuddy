load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["redis.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/util/cache_metrics:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_go_redis_redis_v8//:go_default_library",
    ],
)

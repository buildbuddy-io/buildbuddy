load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "redis_metrics_collector",
    srcs = ["redis_metrics_collector.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_metrics_collector",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//enterprise/server/util/redisutil",
        "//server/environment",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

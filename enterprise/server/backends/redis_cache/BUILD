load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "redis_cache",
    srcs = ["redis_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/cache_metrics",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

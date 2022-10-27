load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "redis_cache",
    srcs = ["redis_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_cache",
    deps = [
        "//enterprise/server/backends/redis_client",
        "//enterprise/server/composable_cache",
        "//enterprise/server/util/redisutil",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/cache_metrics",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

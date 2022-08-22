load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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

go_test(
    name = "redis_metrics_collector_test",
    size = "small",
    srcs = ["redis_metrics_collector_test.go"],
    deps = [
        ":redis_metrics_collector",
        "//enterprise/server/testutil/testredis",
        "//enterprise/server/util/redisutil",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_stretchr_testify//require",
    ],
)

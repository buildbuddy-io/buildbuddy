load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "pubsub",
    srcs = ["pubsub.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/pubsub",
    deps = [
        "//server/interfaces",
        "//server/util/alert",
        "//server/util/log",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

go_test(
    name = "pubsub_test",
    size = "small",
    srcs = ["pubsub_test.go"],
    embed = [":pubsub"],
    deps = [
        "//enterprise/server/testutil/testredis",
        "//enterprise/server/util/redisutil",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

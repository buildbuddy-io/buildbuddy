load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "redis_kvstore",
    srcs = ["redis_kvstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_kvstore",
    deps = [
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

go_test(
    name = "redis_kvstore_test",
    srcs = ["redis_kvstore_test.go"],
    deps = [
        ":redis_kvstore",
        "//enterprise/server/testutil/testredis",
        "//server/util/status",
        "@com_github_stretchr_testify//require",
    ],
)

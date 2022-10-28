load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "redisutil",
    srcs = ["redisutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil",
    deps = [
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@com_github_go_redis_redis_extra_redisotel_v8//:redisotel",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_go_redsync_redsync_v4//:redsync",
        "@com_github_go_redsync_redsync_v4//redis/goredis/v8:goredis",
    ],
)

go_test(
    name = "redisutil_test",
    size = "small",
    srcs = ["redisutil_test.go"],
    deps = [
        ":redisutil",
        "//enterprise/server/testutil/testredis",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_google_go_cmp//cmp",
        "@com_github_google_go_cmp//cmp/cmpopts",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

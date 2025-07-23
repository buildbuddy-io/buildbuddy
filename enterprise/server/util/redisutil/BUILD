load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "redisutil",
    srcs = ["redisutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil",
    deps = [
        "//server/interfaces",
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "@com_github_redis_go_redis_extra_redisotel_v9//:redisotel",
        "@com_github_redis_go_redis_v9//:go-redis",
    ],
)

go_test(
    name = "redisutil_test",
    size = "small",
    srcs = ["redisutil_test.go"],
    deps = [
        ":redisutil",
        "//enterprise/server/testutil/testredis",
        "//server/util/status",
        "@com_github_google_go_cmp//cmp",
        "@com_github_google_go_cmp//cmp/cmpopts",
        "@com_github_redis_go_redis_v9//:go-redis",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

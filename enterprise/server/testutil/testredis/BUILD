load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testredis",
    testonly = 1,
    srcs = ["testredis.go"],
    data = ["@redis//:redis-server"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis",
    x_defs = {
        "redisBinRunfilePath": "$(rlocationpath @redis//:redis-server)",
    },
    deps = [
        "//enterprise/server/util/redisutil",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

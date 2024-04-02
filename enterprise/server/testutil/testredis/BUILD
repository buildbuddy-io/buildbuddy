load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testredis",
    testonly = 1,
    srcs = ["testredis.go"],
    data = [":redis-server_crossplatform"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis",
    x_defs = {
        "redisBinRunfilePath": "$(rlocationpath :redis-server_crossplatform)",
    },
    deps = [
        "//enterprise/server/util/redisutil",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles:go_default_library",
    ],
)

genrule(
    name = "redis-server_crossplatform",
    srcs = select({
        "//platforms/configs:linux_x86_64": ["@com_github_redis_redis-redis-server-v6.2.1-linux-x86_64//file:downloaded"],
        "//platforms/configs:linux_arm64": ["@com_github_redis_redis-redis-server-v6.2.1-linux-arm64//file:downloaded"],
        "//platforms/configs:macos_x86_64": ["@com_github_redis_redis-redis-server-v6.2.6-darwin-x86_64//file:downloaded"],
        "//platforms/configs:macos_arm64": ["@com_github_redis_redis-redis-server-v6.2.6-darwin-arm64//file:downloaded"],
    }),
    outs = ["redis-server"],
    cmd_bash = "cp $(SRCS) $@",
)

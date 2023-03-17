load("@io_bazel_rules_go//go:def.bzl", "go_library")

# TODO(sluongng): ensure all //enterprise code are not dependency of FOSS code
# gazelle:default_visibility //enterprise:__subpackages__,//server/util/dsingleflight:__pkg__
package(default_visibility = [
    "//enterprise:__subpackages__",
    "//server/util/dsingleflight:__pkg__",
])

go_library(
    name = "testredis",
    testonly = 1,
    srcs = ["testredis.go"],
    data = [":redis-server_crossplatform"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis",
    deps = [
        "//enterprise/server/util/redisutil",
        "//server/testutil/testfs",
        "//server/testutil/testport",
        "//server/util/log",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

genrule(
    name = "redis-server_crossplatform",
    srcs = select({
        "//platforms:linux_x86_64": ["@com_github_redis_redis-redis-server-v6.2.1-linux-x86_64//file:downloaded"],
        "//platforms:macos_x86_64": ["@com_github_redis_redis-redis-server-v6.2.6-darwin-x86_64//file:downloaded"],
        "//platforms:macos_arm64": ["@com_github_redis_redis-redis-server-v6.2.6-darwin-arm64//file:downloaded"],
    }),
    outs = ["redis-server"],
    cmd_bash = "cp $(SRCS) $@",
)

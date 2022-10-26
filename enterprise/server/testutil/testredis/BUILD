load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = [
    "//enterprise:__subpackages__",
    "//server/util/dsingleflight:__pkg__",
])

go_library(
    name = "testredis",
    testonly = 1,
    srcs = ["testredis.go"],
    data = [
        "//enterprise/server/test/bin/redis:redis-server-darwin-arm64",
        "//enterprise/server/test/bin/redis:redis-server-linux-x86_64",
    ],
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

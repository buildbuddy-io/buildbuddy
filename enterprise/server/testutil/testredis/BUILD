load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testredis",
    testonly = 1,
    srcs = ["testredis.go"],
    data = ["//enterprise/server/test/bin/redis:redis-server-linux-x86_64"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/redisutil",
        "//server/testutil/app",
        "//server/util/log",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_stretchr_testify//assert",
        "@io_bazel_rules_go//go/tools/bazel:go_default_library",
    ],
)

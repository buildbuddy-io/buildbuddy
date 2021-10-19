load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "usage",
    srcs = ["usage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/util/redisutil",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/status",
        "//server/util/timeutil",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

go_test(
    name = "usage_test",
    srcs = ["usage_test.go"],
    deps = [
        ":usage",
        "//enterprise/server/testutil/testredis",
        "//enterprise/server/util/redisutil",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testclock",
        "//server/testutil/testenv",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_gorm_gorm//:gorm",
        "@org_golang_x_sync//errgroup",
    ],
)

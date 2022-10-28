load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usage",
    srcs = ["usage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage",
    deps = [
        "//enterprise/server/usage/config",
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
    size = "small",
    srcs = ["usage_test.go"],
    deps = [
        ":usage",
        "//enterprise/server/backends/redis_metrics_collector",
        "//enterprise/server/testutil/testredis",
        "//enterprise/server/util/redisutil",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testclock",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_go_sql_driver_mysql//:mysql",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_gorm_gorm//:gorm",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "usage_test_mysql",
    srcs = ["usage_test.go"],
    args = [
        "--testenv.use_mysql",
        "--testmysql.reuse_server",
    ],
    exec_properties = {
        "workload-isolation-type": "firecracker",
        "init-dockerd": "true",
        "recycle-runner": "true",
    },
    shard_count = 7,
    tags = ["docker"],
    deps = [
        ":usage",
        "//enterprise/server/backends/redis_metrics_collector",
        "//enterprise/server/testutil/testredis",
        "//enterprise/server/util/redisutil",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testclock",
        "//server/testutil/testenv",
        "//server/testutil/testmysql",
        "//server/util/testing/flags",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_go_sql_driver_mysql//:mysql",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_gorm_gorm//:gorm",
        "@org_golang_x_sync//errgroup",
    ],
)

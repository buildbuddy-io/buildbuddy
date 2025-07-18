load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usage",
    srcs = ["usage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage",
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//enterprise/server/usage/config",
        "//enterprise/server/util/redisutil",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/tables",
        "//server/util/alert",
        "//server/util/authutil",
        "//server/util/db",
        "//server/util/log",
        "//server/util/status",
        "//server/util/usageutil",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_prometheus_client_golang//prometheus",
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
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/db",
        "//server/util/testing/flags",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_go_sql_driver_mysql//:mysql",
        "@com_github_jonboulle_clockwork//:clockwork",
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
        "--testenv.database_type=mysql",
        "--testenv.reuse_server",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        # We don't want different different db tests to be assigned to the samed
        # recycled runner, because we can't fit all db docker images with the
        # default disk limit.
        "test.runner-recycling-key": "mysql",
    },
    shard_count = 7,
    tags = ["docker"],
    deps = [
        ":usage",
        "//enterprise/server/backends/redis_metrics_collector",
        "//enterprise/server/testutil/testredis",
        "//enterprise/server/util/redisutil",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/db",
        "//server/util/testing/flags",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_go_sql_driver_mysql//:mysql",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_gorm_gorm//:gorm",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "usage_test_postgres",
    srcs = ["usage_test.go"],
    args = [
        "--testenv.database_type=postgres",
        "--testenv.reuse_server",
    ],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        # We don't want different different db tests to be assigned to the samed
        # recycled runner, because we can't fit all db docker images with the
        # default disk limit.
        "test.runner-recycling-key": "postgres",
    },
    shard_count = 7,
    tags = ["docker"],
    deps = [
        ":usage",
        "//enterprise/server/backends/redis_metrics_collector",
        "//enterprise/server/testutil/testredis",
        "//enterprise/server/util/redisutil",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/db",
        "//server/util/testing/flags",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_go_sql_driver_mysql//:mysql",
        "@com_github_jonboulle_clockwork//:clockwork",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_gorm_gorm//:gorm",
        "@org_golang_x_sync//errgroup",
    ],
)

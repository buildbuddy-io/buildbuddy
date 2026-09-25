load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usage_backfill_lib",
    srcs = ["usage_backfill.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/usage_backfill",
    deps = [
        "//server/tables",
        "//server/usage/sku",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/log",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_clickhouse_clickhouse_go_v2//lib/column/orderedmap",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_sync//singleflight",
    ],
)

go_binary(
    name = "usage_backfill",
    embed = [":usage_backfill_lib"],
)

go_test(
    name = "usage_backfill_test",
    srcs = ["usage_backfill_test.go"],
    data = [":usage_backfill"],
    exec_properties = {
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3,mysql8.4.9",
        "test.workload-isolation-type": "firecracker",
    },
    tags = ["docker"],
    x_defs = {
        "usageBackfillRunfilePath": "$(rlocationpath :usage_backfill)",
    },
    deps = [
        "//server/real_environment",
        "//server/tables",
        "//server/testutil/testclickhouse",
        "//server/testutil/testmysql",
        "//server/usage/sku",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/healthcheck",
        "//server/util/testing/flags",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_clickhouse_clickhouse_go_v2//lib/column/orderedmap",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
        "@io_gorm_driver_clickhouse//:clickhouse",
        "@io_gorm_gorm//:gorm",
    ],
)

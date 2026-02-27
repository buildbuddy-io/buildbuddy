load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "clickhouse_backup_lib",
    srcs = ["clickhouse_backup.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/clickhouse_backup",
    visibility = ["//visibility:private"],
    deps = [
        "//server/backends/blobstore",
        "//server/config",
        "//server/environment",
        "//server/real_environment",
        "//server/util/clickhouse",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/log",
    ],
)

go_binary(
    name = "clickhouse_backup",
    embed = [":clickhouse_backup_lib"],
    pure = "on",
    static = "on",
    visibility = ["//visibility:public"],
)

go_test(
    name = "clickhouse_backup_test",
    srcs = ["clickhouse_backup_test.go"],
    data = [
        ":clickhouse_backup",
        "//enterprise/tools/clickhouse_cluster",
    ],
    embed = [":clickhouse_backup_lib"],
    exec_properties = {
        "test.EstimatedComputeUnits": "4",
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3",
    },
    tags = ["docker"],
    x_defs = {
        "clickhouseClusterRlocationpath": "$(rlocationpath //enterprise/tools/clickhouse_cluster:clickhouse_cluster)",
        "clickhouseBackupRlocationpath": "$(rlocationpath :clickhouse_backup)",
    },
    deps = [
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

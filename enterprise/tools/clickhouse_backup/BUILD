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
    data = [":clickhouse_backup"],
    embed = [":clickhouse_backup_lib"],
    exec_properties = {
        "test.EstimatedComputeUnits": "4",
    },
    x_defs = {
        "clickhouseBackupRlocationpath": "$(rlocationpath :clickhouse_backup)",
    },
    deps = [
        "//enterprise/server/testutil/testclickhousecluster",
        "//server/testutil/testenv",
        "//server/testutil/testfs",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

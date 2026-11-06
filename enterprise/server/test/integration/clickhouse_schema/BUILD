load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "clickhouse_schema_test",
    srcs = ["clickhouse_schema_test.go"],
    deps = [
        "//server/testutil/testclickhouse",
        "//server/usage/sku",
        "//server/util/clickhouse/schema",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_clickhouse_clickhouse_go_v2//lib/column/orderedmap",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_gorm_driver_clickhouse//:clickhouse",
        "@io_gorm_gorm//:gorm",
    ],
)

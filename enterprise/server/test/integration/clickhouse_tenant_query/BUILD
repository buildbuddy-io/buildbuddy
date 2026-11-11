load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "clickhouse_tenant_query_test",
    srcs = ["clickhouse_tenant_query_test.go"],
    data = ["//enterprise/tools/clickhouse_cluster"],
    exec_properties = {
        "test.EstimatedComputeUnits": "4",
    },
    x_defs = {
        "clickhouseClusterRlocationpath": "$(rlocationpath //enterprise/tools/clickhouse_cluster:clickhouse_cluster)",
    },
    deps = [
        "//server/testutil/testclickhouse",
        "//server/testutil/testenv",
        "//server/util/clickhouse",
        "//server/util/testing/flags",
        "@com_github_clickhouse_ch_go//proto",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

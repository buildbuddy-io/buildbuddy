load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "clickhouse_tenant_query_test",
    srcs = ["clickhouse_tenant_query_test.go"],
    exec_properties = {
        "test.EstimatedComputeUnits": "4",
    },
    deps = [
        "//enterprise/server/testutil/testclickhousecluster",
        "//server/testutil/testclickhouse",
        "//server/testutil/testenv",
        "//server/util/clickhouse",
        "//server/util/testing/flags",
        "@com_github_clickhouse_ch_go//proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

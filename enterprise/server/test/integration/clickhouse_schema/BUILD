load("@io_bazel_rules_go//go:def.bzl", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "clickhouse_schema_test",
    srcs = ["clickhouse_schema_test.go"],
    exec_properties = {
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3",
        "test.workload-isolation-type": "firecracker",
    },
    tags = ["docker"],
    deps = [
        "//server/testutil/testclickhouse",
        "//server/usage/sku",
        "//server/util/clickhouse/schema",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
        "@io_gorm_driver_clickhouse//:clickhouse",
        "@io_gorm_gorm//:gorm",
    ],
)

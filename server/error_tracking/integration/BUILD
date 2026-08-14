load("@io_bazel_rules_go//go:def.bzl", "go_test")

# Keep Gazelle from adding a second test target without the required Docker
# execution properties.
# gazelle:exclude error_tracking_clickhouse_test.go

go_test(
    name = "error_tracking_clickhouse_test",
    srcs = ["error_tracking_clickhouse_test.go"],  # keep
    exec_properties = {
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3",
        "test.workload-isolation-type": "firecracker",
    },
    tags = ["docker"],
    deps = [
        "//proto:error_tracking_go_proto",
        "//proto:user_id_go_proto",
        "//server/error_tracking",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/clickhouse/schema",
        "//server/util/perms",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_stretchr_testify//require",
    ],
)

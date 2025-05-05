load("//rules/webdriver:index.bzl", "go_web_test_suite")

package(default_visibility = ["//enterprise:__subpackages__"])

go_web_test_suite(
    name = "executions_clickhouse_test",
    srcs = ["executions_clickhouse_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse",
        "test.EstimatedComputeUnits": "4",
    },
    shard_count = 1,
    tags = ["docker"],
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/testutil/testexecutor",
        "//server/testutil/testbazel",
        "//server/testutil/testclickhouse",
        "//server/testutil/testfs",
        "//server/testutil/webtester",
        "//server/util/shlex",
        "//server/util/uuid",
        "@com_github_stretchr_testify//require",
        "@org_golang_x_sync//errgroup",
    ],
)

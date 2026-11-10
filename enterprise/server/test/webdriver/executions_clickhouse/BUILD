load("//rules/webdriver:index.bzl", "go_web_test_suite")
load("//server/util/bazel:defs.bzl", "bazel_versions_test")

package(default_visibility = ["//enterprise:__subpackages__"])

bazel_versions_test(
    name = "executions_clickhouse_test",
    srcs = ["executions_clickhouse_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.EstimatedComputeUnits": "8",
    },
    rule = go_web_test_suite,
    shard_count = 2,
    deps = [
        "//enterprise/server/testutil/buildbuddy_enterprise",
        "//enterprise/server/testutil/testexecutor",
        "//proto/api/v1:common_go_proto",
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

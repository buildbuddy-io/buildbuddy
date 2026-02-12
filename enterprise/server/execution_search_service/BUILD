load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_search_service",
    srcs = ["execution_search_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_search_service",
    deps = [
        "//enterprise/server/util/execution",
        "//proto:execution_stats_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:stat_filter_go_proto",
        "//server/build_event_protocol/invocation_format",
        "//server/environment",
        "//server/interfaces",
        "//server/util/authutil",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/filter",
        "//server/util/query_builder",
        "//server/util/status",
        "//server/util/uuid",
    ],
)

go_test(
    name = "execution_search_service_test",
    srcs = ["execution_search_service_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3",
    },
    tags = ["docker"],
    deps = [
        ":execution_search_service",
        "//proto:execution_stats_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/clickhouse/schema",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

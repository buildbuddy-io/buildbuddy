load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "invocation_search_service",
    srcs = ["invocation_search_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_search_service",
    deps = [
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:stat_filter_go_proto",
        "//server/build_event_protocol/build_event_handler",
        "//server/build_event_protocol/invocation_format",
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/util/alert",
        "//server/util/blocklist",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/filter",
        "//server/util/git",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/proto",
        "//server/util/query_builder",
        "//server/util/status",
        "//server/util/uuid",
    ],
)

go_test(
    name = "invocation_search_service_test",
    srcs = ["invocation_search_service_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        # We don't want different different db tests to be assigned to the samed
        # recycled runner, because we can't fit all db docker images with the
        # default disk limit.
        "test.runner-recycling-key": "clickhouse",
    },
    tags = ["docker"],
    deps = [
        ":invocation_search_service",
        "//proto:context_go_proto",
        "//proto:invocation_go_proto",
        "//proto:stat_filter_go_proto",
        "//server/real_environment",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/perms",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

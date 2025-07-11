load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "invocation_stat_service",
    srcs = ["invocation_stat_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service",
    deps = [
        "//proto:context_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:stat_filter_go_proto",
        "//proto:stats_go_proto",
        "//server/build_event_protocol/invocation_format",
        "//server/environment",
        "//server/interfaces",
        "//server/invocation_stat_service/config",
        "//server/util/authutil",
        "//server/util/db",
        "//server/util/filter",
        "//server/util/git",
        "//server/util/query_builder",
        "//server/util/status",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "invocation_stat_service_test",
    srcs = ["invocation_stat_service_test.go"],
    embed = [":invocation_stat_service"],
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
        "//proto:context_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:stats_go_proto",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/clickhouse/schema",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)

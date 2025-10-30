load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_service",
    srcs = ["execution_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service",
    deps = [
        "//enterprise/server/util/execution",
        "//proto:buildbuddy_service_go_proto",
        "//proto:execution_stats_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//proto:user_id_go_proto",
        "//server/environment",
        "//server/tables",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/rexec",
        "//server/util/status",
        "//server/util/timeseries",
        "//server/util/trace_events",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

go_test(
    name = "execution_service_test",
    srcs = ["execution_service_test.go"],
    exec_properties = {
        "test.workload-isolation-type": "firecracker",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3",
    },
    tags = ["docker"],
    deps = [
        ":execution_service",
        "//enterprise/server/backends/redis_execution_collector",
        "//enterprise/server/testutil/testredis",
        "//proto:execution_stats_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/util/clickhouse/schema",
        "//server/util/perms",
        "//server/util/status",
        "//server/util/testing/flags",
        "//server/util/uuid",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//require",
        "@org_golang_google_genproto_googleapis_rpc//status",
        "@org_golang_google_protobuf//testing/protocmp",
    ],
)

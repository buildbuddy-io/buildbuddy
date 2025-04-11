load("@io_bazel_rules_go//go:def.bzl", "go_library")

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
        "//server/environment",
        "//server/tables",
        "//server/util/authutil",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "//server/util/timeseries",
        "//server/util/trace_events",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

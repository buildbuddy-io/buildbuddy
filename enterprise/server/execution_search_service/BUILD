load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "execution_search_service",
    srcs = ["execution_search_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_search_service",
    deps = [
        "//proto:execution_stats_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/blocklist",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/filter",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "@go_googleapis//google/rpc:status_go_proto",
        "@org_golang_google_grpc//codes",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
    ],
)

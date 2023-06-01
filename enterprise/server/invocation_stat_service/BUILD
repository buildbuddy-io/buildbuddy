load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "invocation_stat_service",
    srcs = ["invocation_stat_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/invocation_stat_service",
    deps = [
        "//enterprise/server/invocation_stat_service/config",
        "//proto:context_go_proto",
        "//proto:invocation_go_proto",
        "//proto:invocation_status_go_proto",
        "//proto:stat_filter_go_proto",
        "//proto:stats_go_proto",
        "//server/build_event_protocol/invocation_format",
        "//server/environment",
        "//server/interfaces",
        "//server/util/clickhouse",
        "//server/util/db",
        "//server/util/filter",
        "//server/util/log",
        "//server/util/perms",
        "//server/util/query_builder",
        "//server/util/status",
        "@org_golang_google_protobuf//proto",
        "@org_golang_google_protobuf//types/known/timestamppb",
        "@org_golang_x_sync//errgroup",
    ],
)

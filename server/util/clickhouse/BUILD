load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "clickhouse",
    srcs = ["clickhouse.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/build_event_protocol/invocation_format",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/tables",
        "//server/util/clickhouse/schema",
        "//server/util/flagutil",
        "//server/util/gormutil",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_prometheus_client_golang//prometheus",
        "@io_gorm_driver_clickhouse//:clickhouse",
        "@io_gorm_gorm//:gorm",
        "@io_gorm_gorm//logger",
    ],
)

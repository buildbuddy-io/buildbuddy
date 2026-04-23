load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "clickhouse",
    srcs = ["clickhouse.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/build_event_protocol/invocation_format",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/remote_cache/digest",
        "//server/tables",
        "//server/util/clickhouse/schema",
        "//server/util/flag",
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

go_test(
    name = "clickhouse_test",
    srcs = ["clickhouse_test.go"],
    deps = [
        ":clickhouse",
        "//proto:remote_execution_go_proto",
        "//server/remote_cache/digest",
        "//server/util/clickhouse/schema",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "clickhouse",
    srcs = ["clickhouse.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/environment",
        "//server/metrics",
        "//server/tables",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/retry",
        "//server/util/status",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@com_github_prometheus_client_golang//prometheus",
        "@io_gorm_driver_clickhouse//:clickhouse",
        "@io_gorm_gorm//:gorm",
    ],
)

go_test(
    name = "clickhouse_test",
    srcs = ["clickhouse_test.go"],
    deps = [
        ":clickhouse",
        "//server/tables",
        "@com_github_go_faker_faker_v4//:faker",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

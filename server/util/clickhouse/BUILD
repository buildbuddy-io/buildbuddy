load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "clickhouse",
    srcs = ["clickhouse.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/tables",
        "//server/util/log",
        "//server/util/status",
        "@com_github_clickhouse_clickhouse_go_v2//:clickhouse-go",
        "@io_gorm_driver_clickhouse//:clickhouse",
        "@io_gorm_gorm//:gorm",
    ],
)

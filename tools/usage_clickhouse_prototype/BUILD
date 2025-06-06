load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "usage_clickhouse_prototype_lib",
    srcs = ["usage_clickhouse_prototype.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/usage_clickhouse_prototype",
    visibility = ["//visibility:private"],
    deps = [
        "//server/environment",
        "//server/real_environment",
        "//server/tables",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/flag",
        "//server/util/log",
    ],
)

go_binary(
    name = "usage_clickhouse_prototype",
    embed = [":usage_clickhouse_prototype_lib"],
    visibility = ["//visibility:public"],
)

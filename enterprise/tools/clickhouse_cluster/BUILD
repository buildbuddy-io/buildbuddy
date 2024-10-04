load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "clickhouse_cluster_lib",
    srcs = ["clickhouse_cluster.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/clickhouse_cluster",
    deps = [
        "//server/util/log",
    ],
)

go_binary(
    name = "clickhouse_cluster",
    embed = [":clickhouse_cluster_lib"],
)

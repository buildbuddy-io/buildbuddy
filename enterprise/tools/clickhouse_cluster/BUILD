load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "clickhouse_cluster_lib",
    srcs = ["clickhouse_cluster.go"],
    data = ["//tools/clickhouse_bin"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/clickhouse_cluster",
    x_defs = {
        "clickhouseRlocationpath": "$(rlocationpath //tools/clickhouse_bin)",
    },
    deps = [
        "//server/util/flag",
        "//server/util/log",
        "@io_bazel_rules_go//go/runfiles",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "clickhouse_cluster",
    embed = [":clickhouse_cluster_lib"],
)

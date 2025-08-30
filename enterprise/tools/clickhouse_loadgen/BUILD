load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "clickhouse_loadgen_lib",
    srcs = ["clickhouse_loadgen.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/clickhouse_loadgen",
    visibility = ["//visibility:private"],
    deps = [
        "//server/environment",
        "//server/real_environment",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/flagutil",
        "//server/util/healthcheck",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/qps",
        "//server/util/uuid",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_time//rate",
    ],
)

go_binary(
    name = "clickhouse_loadgen",
    embed = [":clickhouse_loadgen_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "load_executions_lib",
    srcs = ["load_executions.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/load_executions",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/util/execution",
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/real_environment",
        "//server/tables",
        "//server/util/clickhouse",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/monitoring",
        "//server/util/qps",
        "@com_github_goccy_go_json//:go-json",
        "@com_github_klauspost_compress//zstd",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "load_executions",
    embed = [":load_executions_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])

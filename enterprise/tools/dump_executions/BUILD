load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "dump_executions_lib",
    srcs = ["dump_executions.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/dump_executions",
    visibility = ["//visibility:private"],
    deps = [
        "//server/real_environment",
        "//server/tables",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/healthcheck",
        "//server/util/log",
        "@com_github_klauspost_compress//zstd",
        "@org_golang_x_sync//errgroup",
    ],
)

go_binary(
    name = "dump_executions",
    embed = [":dump_executions_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])

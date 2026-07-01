load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "invocation_backfill_lib",
    srcs = ["invocation_backfill.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/invocation_backfill",
    visibility = ["//visibility:private"],
    deps = [
        "//server/config",
        "//server/real_environment",
        "//server/tables",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/healthcheck",
        "//server/util/lib/set",
        "//server/util/log",
        "@com_github_google_uuid//:uuid",
        "@org_golang_x_sync//errgroup",
        "@org_golang_x_time//rate",
    ],
)

go_binary(
    name = "invocation_backfill",
    embed = [":invocation_backfill_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])

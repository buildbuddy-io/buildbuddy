load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "metronome_exporter_lib",
    srcs = ["metronome_exporter.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/tools/metronome_exporter",
    visibility = ["//visibility:private"],
    deps = [
        "//enterprise/server/backends/configsecrets",
        "//enterprise/server/billing/metronome",
        "//server/config",
        "//server/real_environment",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/db",
        "//server/util/flag",
        "//server/util/flagutil",
        "//server/util/healthcheck",
        "//server/util/log",
    ],
)

go_binary(
    name = "metronome_exporter",
    embed = [":metronome_exporter_lib"],
    visibility = ["//visibility:public"],
)

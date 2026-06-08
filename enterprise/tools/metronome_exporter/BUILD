load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library", "go_test")

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

go_test(
    name = "metronome_exporter_test",
    size = "medium",
    srcs = ["metronome_exporter_test.go"],
    embed = [":metronome_exporter_lib"],
    exec_properties = {
        "test.EstimatedComputeUnits": "4",
        "test.init-dockerd": "true",
        "test.recycle-runner": "true",
        "test.runner-recycling-key": "clickhouse25.3",
        "test.workload-isolation-type": "firecracker",
    },
    tags = ["docker"],
    deps = [
        "//server/real_environment",
        "//server/testutil/testclickhouse",
        "//server/usage/sku",
        "//server/util/clickhouse",
        "//server/util/clickhouse/schema",
        "//server/util/healthcheck",
        "//server/util/testing/flags",
        "@com_github_clickhouse_clickhouse_go_v2//lib/column/orderedmap",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "disk_performance_lib",
    srcs = ["disk_performance.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/disk_performance",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//common",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "disk_performance",
    embed = [":disk_performance_lib"],
)

genrule(
    name = "disk_performance_json",
    outs = ["disk-performance.json"],
    cmd = "$(location :disk_performance) > $@",
    tools = [":disk_performance"],
)

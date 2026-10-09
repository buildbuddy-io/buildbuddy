load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "traffic_stats_lib",
    srcs = ["traffic_stats.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/traffic_stats",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//common",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "traffic_stats",
    embed = [":traffic_stats_lib"],
)

genrule(
    name = "traffic_stats_json",
    outs = ["traffic-stats.json"],
    cmd = "$(execpath :traffic_stats) > $@",
    tools = [":traffic_stats"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "buildbuddy_lib",
    srcs = ["buildbuddy.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/buildbuddy",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//cog",
        "@com_github_grafana_grafana_foundation_sdk_go//common",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//heatmap",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "buildbuddy",
    embed = [":buildbuddy_lib"],
)

genrule(
    name = "buildbuddy_json",
    outs = ["buildbuddy.json"],
    cmd = "$(location :buildbuddy) > $@",
    tools = [":buildbuddy"],
)

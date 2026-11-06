load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "pebble_dash",
    srcs = ["pebble_dash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/pebble_dash",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//common",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

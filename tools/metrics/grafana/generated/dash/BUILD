load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "dash",
    srcs = ["dash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/dash",
    deps = [
        "@com_github_grafana_grafana_foundation_sdk_go//common",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//prometheus",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

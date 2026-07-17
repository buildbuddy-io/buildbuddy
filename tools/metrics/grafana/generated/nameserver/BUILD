load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "nameserver_lib",
    srcs = ["nameserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/nameserver",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//heatmap",
        "@com_github_grafana_grafana_foundation_sdk_go//prometheus",
        "@com_github_grafana_grafana_foundation_sdk_go//stat",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "nameserver",
    embed = [":nameserver_lib"],
)

genrule(
    name = "nameserver_json",
    outs = ["nameserver.json"],
    cmd = "$(location :nameserver) > $@",
    tools = [":nameserver"],
)

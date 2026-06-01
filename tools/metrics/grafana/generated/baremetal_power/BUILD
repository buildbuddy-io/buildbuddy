load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "baremetal_power_lib",
    srcs = ["baremetal_power.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/baremetal_power",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//common",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "baremetal_power",
    embed = [":baremetal_power_lib"],
)

genrule(
    name = "baremetal_power_json",
    outs = ["baremetal-power.json"],
    cmd = "$(location :baremetal_power) > $@",
    tools = [":baremetal_power"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "baremetal_networking_lib",
    srcs = ["baremetal_networking.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/baremetal_networking",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "baremetal_networking",
    embed = [":baremetal_networking_lib"],
)

genrule(
    name = "baremetal_networking_json",
    outs = ["baremetal-networking.json"],
    cmd = "$(location :baremetal_networking) > $@",
    tools = [":baremetal_networking"],
)

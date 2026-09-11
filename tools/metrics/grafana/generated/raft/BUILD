load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "raft_lib",
    srcs = ["raft.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/raft",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//cog",
        "@com_github_grafana_grafana_foundation_sdk_go//common",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//heatmap",
        "@com_github_grafana_grafana_foundation_sdk_go//prometheus",
        "@com_github_grafana_grafana_foundation_sdk_go//table",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "raft",
    embed = [":raft_lib"],
)

genrule(
    name = "raft_json",
    outs = ["raft.json"],
    cmd = "$(location :raft) > $@",
    tools = [":raft"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "go_runtime_lib",
    srcs = ["go_runtime.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/go_runtime",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//heatmap",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "go_runtime",
    embed = [":go_runtime_lib"],
)

genrule(
    name = "go_runtime_json",
    outs = ["go-runtime.json"],
    cmd = "$(location :go_runtime) > $@",
    tools = [":go_runtime"],
)

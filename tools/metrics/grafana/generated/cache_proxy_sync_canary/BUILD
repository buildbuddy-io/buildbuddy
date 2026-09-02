load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "cache_proxy_sync_canary_lib",
    srcs = ["cache_proxy_sync_canary.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/metrics/grafana/generated/cache_proxy_sync_canary",
    deps = [
        "//tools/metrics/grafana/generated/dash",
        "@com_github_grafana_grafana_foundation_sdk_go//dashboard",
        "@com_github_grafana_grafana_foundation_sdk_go//timeseries",
    ],
)

go_binary(
    name = "cache_proxy_sync_canary",
    embed = [":cache_proxy_sync_canary_lib"],
)

genrule(
    name = "cache_proxy_sync_canary_json",
    outs = ["cache-proxy-sync-canary.json"],
    cmd = "$(location :cache_proxy_sync_canary) > $@",
    tools = [":cache_proxy_sync_canary"],
)

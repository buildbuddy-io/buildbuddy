load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "prom",
    srcs = ["prom.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/prom",
    deps = [
        "//server/environment",
        "//server/metrics",
        "//server/util/log",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_prometheus_client_golang//api",
        "@com_github_prometheus_client_golang//api/prometheus/v1:prometheus",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_common//model",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

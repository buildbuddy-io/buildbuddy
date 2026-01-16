load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

go_library(
    name = "metrics",
    srcs = ["metrics.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/metrics",
    deps = [
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_prometheus_client_golang//prometheus/promauto",
    ],
)

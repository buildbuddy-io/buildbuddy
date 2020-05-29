load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["monitoring.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/monitoring",
    visibility = ["//visibility:public"],
    deps = ["@com_github_prometheus_client_golang//prometheus/promhttp:go_default_library"],
)

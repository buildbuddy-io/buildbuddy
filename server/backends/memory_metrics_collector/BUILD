load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "memory_metrics_collector",
    srcs = ["memory_metrics_collector.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector",
    visibility = ["//visibility:public"],
    deps = ["@com_github_hashicorp_golang_lru//:golang-lru"],
)

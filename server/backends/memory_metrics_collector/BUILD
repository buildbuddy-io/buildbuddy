load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "memory_metrics_collector",
    srcs = ["memory_metrics_collector.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/memory_metrics_collector",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_hashicorp_golang_lru//:golang-lru",
    ],
)

go_test(
    name = "memory_metrics_collector_test",
    size = "small",
    srcs = ["memory_metrics_collector_test.go"],
    embed = [":memory_metrics_collector"],
    deps = ["@com_github_stretchr_testify//require"],
)

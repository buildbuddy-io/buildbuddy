load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "hit_tracker",
    srcs = ["hit_tracker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:cache_go_proto",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/bazel_request",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

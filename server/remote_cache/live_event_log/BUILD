load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "live_event_log",
    srcs = ["live_event_log.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/remote_cache/live_event_log",
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

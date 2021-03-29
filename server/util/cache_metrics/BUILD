load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "cache_metrics",
    srcs = ["cache_metrics.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//status",
    ],
)

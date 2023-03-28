load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "metric",
    srcs = ["metric.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/metric",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_google_grpc//status",
    ],
)

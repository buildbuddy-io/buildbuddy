load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["cache_metrics.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/cache_metrics",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics:go_default_library",
        "@com_github_prometheus_client_golang//prometheus:go_default_library",
        "@org_golang_google_grpc//status:go_default_library",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "httpclient",
    srcs = ["httpclient.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/http/httpclient",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/log",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_x_net//publicsuffix",
    ],
)

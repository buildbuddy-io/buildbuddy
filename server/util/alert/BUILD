load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "alert",
    srcs = ["alert.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/alert",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/log",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "monitoring",
    srcs = ["monitoring.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/monitoring",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/basicauth",
        "//server/util/flag",
        "//server/util/flagz",
        "//server/util/grpc_server",
        "//server/util/log",
        "//server/util/status",
        "//server/util/statusz",
        "@com_github_prometheus_client_golang//prometheus/promhttp",
        "@com_github_rantav_go_grpc_channelz//:go-grpc-channelz",
    ],
)

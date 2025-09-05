load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "routing_action_cache_client",
    srcs = ["routing_action_cache_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_action_cache_client",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/status",
        "@org_golang_google_grpc//:grpc",
    ],
)

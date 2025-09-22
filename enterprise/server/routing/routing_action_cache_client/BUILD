load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "routing_action_cache_client",
    srcs = ["routing_action_cache_client.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/routing_action_cache_client",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/batch_operator",
        "//proto:remote_execution_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
        "@org_golang_google_grpc//:grpc",
    ],
)

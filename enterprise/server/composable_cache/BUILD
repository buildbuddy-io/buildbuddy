load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "composable_cache",
    srcs = ["composable_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/composable_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/interfaces",
        "//server/util/status",
    ],
)

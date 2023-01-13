load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "redis_execution_collector",
    srcs = ["redis_execution_collector.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/environment",
        "@com_github_go_redis_redis_v8//:redis",
        "@org_golang_google_protobuf//proto",
    ],
)

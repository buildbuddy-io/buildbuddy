load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "redis_execution_collector",
    srcs = ["redis_execution_collector.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:stored_invocation_go_proto",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/proto",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

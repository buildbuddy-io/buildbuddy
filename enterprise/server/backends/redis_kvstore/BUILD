load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "redis_kvstore",
    srcs = ["redis_kvstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_kvstore",
    deps = [
        "//server/environment",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

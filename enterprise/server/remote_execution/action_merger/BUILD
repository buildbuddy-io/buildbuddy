load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "action_merger",
    srcs = ["action_merger.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/action_merger",
    deps = [
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/log",
        "//server/util/prefix",
        "@com_github_go_redis_redis_v8//:redis",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "executor_config",
    srcs = ["executor_config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/executor_config",
    deps = [
        "//proto:executor_go_proto",
        "//proto:workflow_go_proto",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/background",
        "//server/util/log",
        "//server/util/status",
    ],
)

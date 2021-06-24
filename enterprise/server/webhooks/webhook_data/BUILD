load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "webhook_data",
    srcs = ["webhook_data.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = ["//server/interfaces"],
)

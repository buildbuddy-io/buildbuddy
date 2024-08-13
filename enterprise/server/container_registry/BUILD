load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "container_registry",
    srcs = ["container_registry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/container_registry",
    visibility = ["//visibility:public"],
    deps = [
        "//server/real_environment",
        "//server/util/status",
        "@com_github_distribution_distribution_v3//configuration",
        "@com_github_distribution_distribution_v3//registry/handlers",
    ],
)

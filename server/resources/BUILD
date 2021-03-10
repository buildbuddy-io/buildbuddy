load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["resources.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/resources",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = ["@com_github_elastic_gosigar//:go_default_library"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "resources",
    srcs = ["resources.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/resources",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//server/environment",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing/os",
        "@com_github_elastic_gosigar//:gosigar",
    ],
)

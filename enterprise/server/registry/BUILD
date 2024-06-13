load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "registry",
    srcs = [
        "github_registry.go",
        "npm_registry.go",
        "registry_server.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/registry",
    deps = [
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/log",
    ],
)

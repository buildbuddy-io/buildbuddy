load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "registry",
    srcs = ["registry_server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/registry",
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
    ],
)

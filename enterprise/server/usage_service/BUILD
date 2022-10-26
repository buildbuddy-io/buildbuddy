load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usage_service",
    srcs = ["usage_service.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usage_service",
    deps = [
        "//enterprise/server/usage/config",
        "//proto:usage_go_proto",
        "//server/environment",
        "//server/util/log",
        "//server/util/perms",
    ],
)

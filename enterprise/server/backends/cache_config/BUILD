load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "cache_config",
    srcs = ["cache_config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/cache_config",
    deps = [
        "//server/util/disk",
    ],
)

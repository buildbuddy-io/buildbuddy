load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //cli:__subpackages__,//tools:__subpackages__
package(default_visibility = [
    "//cli:__subpackages__",
    "//tools:__subpackages__",
])

go_library(
    name = "storage",
    srcs = ["storage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/storage",
    deps = ["//cli/workspace"],
)

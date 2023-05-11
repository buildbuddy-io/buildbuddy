load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "storage",
    srcs = ["storage.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/storage",
    deps = ["//cli/workspace"],
)

package(default_visibility = ["//cli:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "workspace",
    srcs = ["workspace.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/workspace",
    deps = [
        "//cli/log",
        "//server/util/disk",
    ],
)

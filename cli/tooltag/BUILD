load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tooltag",
    srcs = ["tooltag.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/tooltag",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/version",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "setup",
    srcs = ["setup.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/setup",
    deps = [
        "//cli/arg",
        "//cli/login",
        "//cli/parser",
        "//cli/plugin",
        "//cli/sidecar",
        "//cli/tooltag",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bazel_command",
    srcs = ["bazel_command.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/bazel_command",
    deps = [
        "//cli/arg",
        "//cli/ask",
        "//cli/bazelisk",
        "//cli/log",
        "//cli/login",
        "//cli/metadata",
        "//cli/parser",
        "//cli/picker",
        "//cli/plugin",
        "//cli/sidecar",
        "//cli/tooltag",
        "//cli/watcher",
    ],
)

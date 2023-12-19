load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "help",
    srcs = ["help.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/help",
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/cli_command",
        "//cli/parser",
        "//cli/version",
        "//server/util/lockingbuffer",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

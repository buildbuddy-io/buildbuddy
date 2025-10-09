load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "picker",
    srcs = ["picker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/picker",
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/log",
        "//cli/terminal",
        "@com_github_manifoldco_promptui//:promptui",
    ],
)

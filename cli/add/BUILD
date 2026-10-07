load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "add",
    srcs = [
        "add.go",
        "bcr.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/add",
    deps = [
        "//cli/arg",
        "//cli/bzlmod",
        "//cli/log",
        "//cli/terminal",
        "//cli/workspace",
        "@com_github_manifoldco_promptui//:promptui",
    ],
)

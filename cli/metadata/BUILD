load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "metadata",
    srcs = ["metadata.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/metadata",
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/parser",
        "//cli/workspace",
        "//server/util/git",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

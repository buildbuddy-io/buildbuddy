load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "bbrc",
    srcs = ["bbrc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/bbrc",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/parser/bazelrc",
        "//cli/parser/options",
        "//cli/parser/parsed",
    ],
)

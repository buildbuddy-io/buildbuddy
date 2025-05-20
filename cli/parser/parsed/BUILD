load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "parsed",
    srcs = ["parsed.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/parsed",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/parser/arguments",
        "//cli/parser/options",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

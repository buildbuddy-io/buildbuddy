load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "why",
    srcs = ["why.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/why",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/log",
        "//cli/why/execgraph",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

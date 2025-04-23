load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "runscript",
    srcs = ["runscript.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/runscript",
    visibility = ["//visibility:public"],
    deps = ["//cli/arg"],
)

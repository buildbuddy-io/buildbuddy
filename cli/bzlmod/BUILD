load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "bzlmod",
    srcs = ["bzlmod.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/bzlmod",
    visibility = ["//visibility:public"],
    deps = ["//cli/bazelisk"],
)

package(default_visibility = ["//cli:__subpackages__"])

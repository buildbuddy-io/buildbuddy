load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "claude",
    srcs = ["claude.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/util/agent/claude",
    deps = ["//cli/terminal"],
)

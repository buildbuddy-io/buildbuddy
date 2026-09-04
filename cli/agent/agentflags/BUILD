load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "agentflags",
    srcs = ["agentflags.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/agent/agentflags",
    deps = [
        "//cli/login",
        "//cli/util/agent/agentutil",
    ],
)

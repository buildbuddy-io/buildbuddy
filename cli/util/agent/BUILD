load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "agent",
    srcs = ["agent.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/util/agent",
    deps = [
        "//cli/util/agent/agentutil",
        "//cli/util/agent/claude",
        "//cli/util/agent/codex",
    ],
)

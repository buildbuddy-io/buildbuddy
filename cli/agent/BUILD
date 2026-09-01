load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "agent",
    srcs = ["agent.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/agent",
    deps = [
        "//cli/agent/agentflags",
        "//cli/agent/analyze_profile",
        "//cli/arg",
        "//cli/log",
    ],
)

go_test(
    name = "agent_test",
    srcs = ["agent_test.go"],
    embed = [":agent"],
    deps = [
        "//cli/agent/agentflags",
        "//cli/login",
        "//cli/util/agent/agentutil",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

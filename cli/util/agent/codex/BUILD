load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "codex",
    srcs = ["codex.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/util/agent/codex",
    deps = ["//cli/util/agent/agentutil"],
)

go_test(
    name = "codex_test",
    srcs = ["codex_test.go"],
    embed = [":codex"],
    deps = ["@com_github_stretchr_testify//require"],
)

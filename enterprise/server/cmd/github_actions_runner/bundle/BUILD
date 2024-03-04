load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

genrule(
    name = "actions_runner",
    srcs = ["//enterprise/server/cmd/github_actions_runner"],
    outs = [":buildbuddy_github_actions_runner"],
    cmd_bash = "cp $(SRCS) $@",
)

# Gazelle does not handle generated embedsrcs properly; ignore this target for now.
# gazelle:ignore
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [":buildbuddy_github_actions_runner"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/github_actions_runner/bundle",
)

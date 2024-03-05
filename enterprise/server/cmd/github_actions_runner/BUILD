load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "github_actions_runner_lib",
    srcs = ["github_actions_runner.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/github_actions_runner",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
    ],
)

go_binary(
    name = "github_actions_runner",
    embed = [":github_actions_runner_lib"],
    pure = "on",
    static = "on",
)

package(default_visibility = ["//enterprise:__subpackages__"])

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "slashcommand",
    srcs = ["slashcommand.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github/slashcommand",
    deps = [
        "//enterprise/server/workflow/config",
        "//proto:runner_go_proto",
    ],
)

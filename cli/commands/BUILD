load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "commands",
    srcs = ["commands.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/commands",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/add",
        "//cli/analyze",
        "//cli/ask",
        "//cli/common",
        "//cli/download",
        "//cli/execute",
        "//cli/fix",
        "//cli/login",
        "//cli/plugin",
        "//cli/printlog",
        "//cli/update",
        "//cli/upload",
        "//cli/version",
    ],
)

package(default_visibility = ["//cli:__subpackages__"])

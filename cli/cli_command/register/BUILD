load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "register",
    srcs = ["register.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/cli_command/register",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/add",
        "//cli/analyze",
        "//cli/ask",
        "//cli/cli_command",
        "//cli/download",
        "//cli/execute",
        "//cli/explain",
        "//cli/fix",
        "//cli/index",
        "//cli/login",
        "//cli/plugin",
        "//cli/printlog",
        "//cli/record",
        "//cli/remote_download",
        "//cli/remotebazel",
        "//cli/search",
        "//cli/ui",
        "//cli/update",
        "//cli/upload",
        "//cli/versioncmd",
        "//cli/view",
    ],
)

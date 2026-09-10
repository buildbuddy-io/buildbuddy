load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "register",
    srcs = ["register.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/cli_command/register",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/add",
        "//cli/agent",
        "//cli/agent/agentflags",
        "//cli/analyze",
        "//cli/ask",
        "//cli/box",
        "//cli/cli_command",
        "//cli/detect",
        "//cli/download",
        "//cli/execute",
        "//cli/execution",
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
        "//cli/ssh",
        "//cli/ssh_server",
        "//cli/ui",
        "//cli/update",
        "//cli/upload",
        "//cli/versioncmd",
        "//cli/view",
    ],
)

go_test(
    name = "register_test",
    srcs = ["register_test.go"],
    deps = [
        ":register",
        "//cli/cli_command",
        "@com_github_stretchr_testify//require",
    ],
)

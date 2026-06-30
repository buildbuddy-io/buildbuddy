load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "install_mcp",
    srcs = ["install_mcp.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/install_mcp",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/login",
        "//cli/storage",
        "//server/util/status",
    ],
)

go_test(
    name = "install_mcp_test",
    srcs = ["install_mcp_test.go"],
    embed = [":install_mcp"],
    deps = ["@com_github_stretchr_testify//require"],
)

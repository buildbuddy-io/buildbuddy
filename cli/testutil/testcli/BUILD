load("@io_bazel_rules_go//go:def.bzl", "go_library")
load("//rules:coverage_transitions.bzl", "coverage_free_tool")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "testcli",
    testonly = 1,
    srcs = ["testcli.go"],
    data = [":bb_no_cov"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli",
    visibility = ["//visibility:public"],
    x_defs = {
        "bbRunfilePath": "$(rlocationpath :bb_no_cov)",
    },
    deps = [
        "//cli/storage",
        "//server/testutil/testbazel",
        "//server/testutil/testbazelisk",
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "//server/util/lockingbuffer",
        "//server/util/terminal",
        "@com_github_creack_pty//:pty",
        "@com_github_stretchr_testify//require",
    ],
)

coverage_free_tool(
    name = "bb_no_cov",
    tags = ["manual"],
    target = "//cli/cmd/bb",
)

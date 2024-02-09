load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testcli",
    testonly = 1,
    srcs = ["testcli.go"],
    data = ["//cli/cmd/bb"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli",
    visibility = ["//visibility:public"],
    x_defs = {
        "bbRunfilePath": "$(rlocationpath //cli/cmd/bb)",
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

package(default_visibility = ["//cli:__subpackages__"])

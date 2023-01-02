load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testcli",
    testonly = 1,
    srcs = ["testcli.go"],
    data = ["//cli/cmd/bb"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/testutil/testcli",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/storage",
        "//server/testutil/testbazel",
        "//server/testutil/testfs",
        "//server/testutil/testgit",
        "@com_github_stretchr_testify//require",
    ],
)

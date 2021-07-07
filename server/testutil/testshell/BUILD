load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testshell",
    testonly = 1,
    srcs = ["testshell.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testshell",
    visibility = ["//visibility:public"],
    deps = ["@com_github_stretchr_testify//require"],
)

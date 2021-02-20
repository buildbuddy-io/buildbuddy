load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["workflowcmd.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/workflowcmd",
    visibility = ["//visibility:public"],
    deps = ["@com_github_google_shlex//:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = ["workflowcmd_test.go"],
    embed = [":go_default_library"],
    deps = ["@com_github_stretchr_testify//assert:go_default_library"],
)

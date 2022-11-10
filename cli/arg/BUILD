load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "arg",
    srcs = ["arg.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/arg",
    visibility = ["//visibility:public"],
)

go_test(
    name = "arg_test",
    srcs = ["arg_test.go"],
    embed = [":arg"],
    deps = ["@com_github_stretchr_testify//assert"],
)

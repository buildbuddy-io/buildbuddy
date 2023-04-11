load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testrand",
    srcs = ["testrand.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testrand",
    visibility = ["//visibility:public"],
    deps = ["@com_github_stretchr_testify//require"],
)

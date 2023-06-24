load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "dockerutil",
    srcs = ["dockerutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/dockerutil",
    visibility = ["//visibility:public"],
    deps = ["@com_github_stretchr_testify//require"],
)

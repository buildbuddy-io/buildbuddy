load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testkeys",
    srcs = ["testkeys.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testkeys",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_stretchr_testify//require",
    ],
)

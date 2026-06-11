load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testhosts",
    testonly = 1,
    srcs = ["testhosts.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testhosts",
    visibility = ["//visibility:public"],
    deps = ["@com_github_stretchr_testify//require"],
)

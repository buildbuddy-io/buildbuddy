load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testdata",
    srcs = ["testdata.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testdata",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "@com_github_stretchr_testify//require",
    ],
)

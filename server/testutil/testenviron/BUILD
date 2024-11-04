load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testenviron",
    testonly = 1,
    srcs = ["testenviron.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testenviron",
    visibility = ["//visibility:public"],
    deps = [
        "@com_github_stretchr_testify//require",
    ],
)

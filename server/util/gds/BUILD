load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "gds",
    srcs = ["gds.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/gds",
    visibility = ["//visibility:public"],
)

go_test(
    name = "gds_test",
    srcs = ["gds_test.go"],
    deps = [
        ":gds",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

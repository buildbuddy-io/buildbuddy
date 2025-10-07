load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "wrand",
    srcs = ["wrand.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/wrand",
    visibility = ["//visibility:public"],
)

go_test(
    name = "wrand_test",
    srcs = ["wrand_test.go"],
    deps = [
        ":wrand",
        "@com_github_stretchr_testify//require",
    ],
)

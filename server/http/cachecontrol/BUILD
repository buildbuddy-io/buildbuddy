load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cachecontrol",
    srcs = ["cachecontrol.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/http/cachecontrol",
    visibility = ["//visibility:public"],
)

go_test(
    name = "cachecontrol_test",
    srcs = ["cachecontrol_test.go"],
    deps = [
        ":cachecontrol",
        "@com_github_stretchr_testify//require",
    ],
)

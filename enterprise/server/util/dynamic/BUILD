load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["dynamic.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/dynamic",
    visibility = ["//visibility:public"],
    deps = ["//server/util/status:go_default_library"],
)

go_test(
    name = "go_default_test",
    srcs = ["dynamic_test.go"],
    deps = [
        ":go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

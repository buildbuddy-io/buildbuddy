load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "cacheopts",
    srcs = ["cacheopts.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/cacheopts",
    visibility = ["//visibility:public"],
)

go_test(
    name = "cacheopts_test",
    srcs = ["cacheopts_test.go"],
    deps = [
        ":cacheopts",
        "@com_github_stretchr_testify//require",
    ],
)

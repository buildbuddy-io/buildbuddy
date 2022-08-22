load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "query_builder",
    srcs = ["query_builder.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/query_builder",
    visibility = ["//visibility:public"],
)

go_test(
    name = "query_builder_test",
    size = "small",
    srcs = ["query_builder_test.go"],
    deps = [
        ":query_builder",
        "@com_github_stretchr_testify//assert",
    ],
)

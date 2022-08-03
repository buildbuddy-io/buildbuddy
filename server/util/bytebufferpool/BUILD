load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "bytebufferpool",
    srcs = ["bytebufferpool.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/bytebufferpool",
    visibility = ["//visibility:public"],
)

go_test(
    name = "bytebufferpool_test",
    size = "small",
    srcs = ["bytebufferpool_test.go"],
    deps = [
        ":bytebufferpool",
        "@com_github_stretchr_testify//assert",
    ],
)

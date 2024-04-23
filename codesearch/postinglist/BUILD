load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "postinglist",
    srcs = ["postinglist.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/postinglist",
    visibility = ["//visibility:public"],
)

go_test(
    name = "postinglist_test",
    srcs = ["postinglist_test.go"],
    deps = [
        ":postinglist",
        "@com_github_stretchr_testify//assert",
    ],
)

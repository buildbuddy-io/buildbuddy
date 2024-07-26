load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "token",
    srcs = ["token.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/token",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/types",
        "//server/util/status",
    ],
)

go_test(
    name = "token_test",
    srcs = ["token_test.go"],
    embed = [":token"],
    deps = [
        "@com_github_roaringbitmap_roaring//:roaring",
        "@com_github_stretchr_testify//assert",
    ],
)

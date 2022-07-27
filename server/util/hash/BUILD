load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "hash",
    srcs = ["hash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/hash",
    visibility = ["//visibility:public"],
)

go_test(
    name = "hash_test",
    size = "small",
    srcs = ["hash_test.go"],
    deps = [
        ":hash",
        "@com_github_stretchr_testify//assert",
    ],
)

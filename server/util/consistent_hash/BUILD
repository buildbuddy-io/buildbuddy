load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["consistent_hash.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/consistent_hash",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["consistent_hash_test.go"],
    embed = [":go_default_library"],
    deps = [
        "//server/util/random:go_default_library",
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

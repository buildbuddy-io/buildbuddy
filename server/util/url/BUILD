load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["url.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/url",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = ["url_test.go"],
    # keep
    embed = [],
    deps = [
        ":go_default_library",  # keep
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "go_default_library",
    srcs = ["git.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/git",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status:go_default_library",
        "@com_github_whilp_git_urls//:go_default_library",
    ],
)

go_test(
    name = "go_default_test",
    srcs = ["git_test.go"],
    # keep
    embed = [],
    deps = [
        ":go_default_library",  # keep
        "@com_github_stretchr_testify//assert:go_default_library",
    ],
)

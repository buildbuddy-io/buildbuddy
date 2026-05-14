load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "set",
    srcs = ["set.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/lib/set",
    visibility = ["//visibility:public"],
)

go_test(
    name = "set_test",
    srcs = ["set_test.go"],
    deps = [
        ":set",
        "@com_github_google_go_cmp//cmp",
        "@com_github_stretchr_testify//assert",
    ],
)

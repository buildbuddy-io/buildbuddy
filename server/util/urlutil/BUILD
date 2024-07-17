load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "urlutil",
    srcs = ["urlutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/urlutil",
    visibility = ["//visibility:public"],
)

go_test(
    name = "urlutil_test",
    srcs = ["urlutil_test.go"],
    deps = [
        ":urlutil",
        "@com_github_stretchr_testify//assert",
    ],
)

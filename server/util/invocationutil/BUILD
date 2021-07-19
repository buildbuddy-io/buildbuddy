load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "invocationutil",
    srcs = ["invocationutil.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/invocationutil",
    visibility = ["//visibility:public"],
    deps = [],
)

go_test(
    name = "invocationutil_test",
    srcs = ["invocationutil_test.go"],
    deps = [
        ":invocationutil",
        "@com_github_stretchr_testify//assert",
    ],
)

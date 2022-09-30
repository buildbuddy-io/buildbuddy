load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "basicauth",
    srcs = ["basicauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/basicauth",
    visibility = ["//visibility:public"],
)

go_test(
    name = "basicauth_test",
    srcs = ["basicauth_test.go"],
    deps = [
        ":basicauth",
        "//server/testutil/testhttp",
        "@com_github_stretchr_testify//require",
    ],
)

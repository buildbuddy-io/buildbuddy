load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "apikey",
    srcs = ["apikey.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/apikey",
    visibility = ["//visibility:public"],
    deps = ["//server/environment"],
)

go_test(
    name = "apikey_test",
    srcs = ["apikey_test.go"],
    deps = [
        ":apikey",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
    ],
)

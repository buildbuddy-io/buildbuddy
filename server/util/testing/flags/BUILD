load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "flags",
    testonly = 1,
    srcs = ["flags.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/testing/flags",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil",
        "//server/util/flagutil/common",
        "//server/util/flagutil/yaml",
        "@com_github_stretchr_testify//require",
    ],
)

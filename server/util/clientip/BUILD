load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "clientip",
    srcs = ["clientip.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/clientip",
    visibility = ["//visibility:public"],
    deps = ["//server/util/flag"],
)

go_test(
    name = "clientip_test",
    srcs = ["clientip_test.go"],
    deps = [
        ":clientip",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "rangemap",
    srcs = ["rangemap.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/v2/server/util/rangemap",
    visibility = ["//visibility:public"],
    deps = ["//server/util/log"],
)

go_test(
    name = "rangemap_test",
    size = "small",
    srcs = ["rangemap_test.go"],
    deps = [
        ":rangemap",
        "@com_github_stretchr_testify//require",
    ],
)

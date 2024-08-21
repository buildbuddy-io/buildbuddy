load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "filters",
    srcs = ["filters.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/filters",
    visibility = ["//visibility:public"],
    deps = ["@com_github_go_enry_go_enry_v2//:go-enry"],
)

go_test(
    name = "filters_test",
    srcs = ["filters_test.go"],
    deps = [
        ":filters",
        "@com_github_stretchr_testify//assert",
    ],
)

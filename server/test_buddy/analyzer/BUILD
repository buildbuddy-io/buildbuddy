load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "analyzer",
    srcs = ["analyzer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/test_buddy/analyzer",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:test_buddy_go_proto",
        "//server/test_buddy/config",
    ],
)

go_test(
    name = "analyzer_test",
    srcs = ["analyzer_test.go"],
    deps = [
        ":analyzer",
        "//proto:test_buddy_go_proto",
        "//server/test_buddy/config",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

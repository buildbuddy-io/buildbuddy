load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "analyzer",
    srcs = ["analyzer.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/test_health/analyzer",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:test_health_go_proto",
        "//server/test_health/config",
    ],
)

go_test(
    name = "analyzer_test",
    srcs = ["analyzer_test.go"],
    deps = [
        ":analyzer",
        "//proto:test_health_go_proto",
        "//server/test_health/config",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

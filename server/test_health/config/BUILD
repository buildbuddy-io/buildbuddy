load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/test_health/config",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:test_health_go_proto",
        "//server/util/status",
    ],
)

go_test(
    name = "config_test",
    srcs = ["config_test.go"],
    deps = [
        ":config",
        "//proto:test_health_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

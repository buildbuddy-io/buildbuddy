load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "normalize",
    srcs = ["normalize.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/test_health/normalize",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:test_health_go_proto",
        "//server/test_health/identity",
        "//server/util/status",
    ],
)

go_test(
    name = "normalize_test",
    srcs = ["normalize_test.go"],
    deps = [
        ":normalize",
        "//proto:test_health_go_proto",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

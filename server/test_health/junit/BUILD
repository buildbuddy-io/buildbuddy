load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "junit",
    srcs = ["junit.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/test_health/junit",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:test_health_go_proto",
        "//server/test_health/identity",
        "//server/test_health/normalize",
        "//server/util/status",
    ],
)

go_test(
    name = "junit_test",
    srcs = ["junit_test.go"],
    deps = [
        ":junit",
        "//proto:test_health_go_proto",
        "//server/util/status",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

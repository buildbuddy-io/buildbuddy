load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "junit",
    srcs = ["junit.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/junit",
    visibility = ["//visibility:public"],
)

go_test(
    name = "junit_test",
    srcs = ["junit_test.go"],
    embed = [":junit"],
    deps = ["@com_github_stretchr_testify//require"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "timeout_fixture",
    srcs = ["timeout_fixture.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/test_buddy/timeout_fixture",
    visibility = ["//visibility:private"],
)

go_test(
    name = "timeout_fixture_test",
    size = "small",
    srcs = ["timeout_fixture_test.go"],
    deps = [":timeout_fixture"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "failing_fixture",
    srcs = ["failing_fixture.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/test_buddy/failing_fixture",
    visibility = ["//visibility:private"],
)

go_test(
    name = "failing_fixture_test",
    srcs = ["failing_fixture_test.go"],
    embed = [":failing_fixture"],
)

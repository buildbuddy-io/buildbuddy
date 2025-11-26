load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")
load("//rules:coverage_transitions.bzl", "coverage_free_tool")

package(
    default_testonly = 1,
    default_visibility = ["//enterprise:__subpackages__"],
)

go_library(
    name = "lib",
    srcs = ["test_binary.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil/test_binary",
)

go_binary(
    name = "test_binary",
    embed = [":lib"],
)

coverage_free_tool(
    name = "test_binary_no_cov",
    tags = ["manual"],
    target = ":test_binary",
)

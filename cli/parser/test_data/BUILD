load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "test_data",
    testonly = 1,
    srcs = ["test_data.go"],
    embedsrcs = ["bazel7.4.0_help_flags-as-proto.b64"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser/test_data",
)

package(default_visibility = ["//cli:__subpackages__"])

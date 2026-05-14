load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testworker_lib",
    srcs = ["testworker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner/testworker",
    visibility = ["//visibility:private"],
    deps = ["//server/util/log"],
)

go_binary(
    name = "testworker",
    embed = [":testworker_lib"],
    pure = "on",
    static = "on",
)

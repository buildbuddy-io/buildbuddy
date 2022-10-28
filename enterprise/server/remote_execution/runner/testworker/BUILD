load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise/server/remote_execution/runner:__subpackages__"])

go_library(
    name = "testworker_lib",
    srcs = ["testworker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner/testworker",
    deps = ["//server/util/log"],
)

go_binary(
    name = "testworker",
    embed = [":testworker_lib"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "sandbox_wrapper_lib",
    srcs = ["sandbox_wrapper.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/linux_sandbox/sandbox_wrapper",
    visibility = ["//visibility:private"],
    deps = ["//server/util/flag"],
)

go_binary(
    name = "sandbox_wrapper",
    embed = [":sandbox_wrapper_lib"],
    visibility = ["//visibility:public"],
)

package(default_visibility = ["//enterprise:__subpackages__"])

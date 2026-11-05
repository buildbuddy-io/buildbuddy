load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testsyscall_lib",
    srcs = ["testsyscall.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/ociruntime/testsyscall",
    target_compatible_with = ["@platforms//os:linux"],
    visibility = ["//visibility:private"],
    deps = ["@org_golang_x_sys//unix"],
)

go_binary(
    name = "testsyscall",
    embed = [":testsyscall_lib"],
    pure = "on",
    static = "on",
)

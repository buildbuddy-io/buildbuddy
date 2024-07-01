load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "bazel_wrapper_lib",
    srcs = ["bazel_wrapper.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bazel_wrapper",
    visibility = ["//visibility:private"],
    deps = [
        "//server/util/log",
        "@com_github_creack_pty//:pty",
    ],
)

go_binary(
    name = "bazel_wrapper",
    embed = [":bazel_wrapper_lib"],
    visibility = ["//visibility:public"],
    pure = "on",
    static = "on",
)

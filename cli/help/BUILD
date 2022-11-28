load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "help",
    srcs = ["help.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/help",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/bazelisk",
        "//cli/parser",
        "//cli/version",
    ],
)

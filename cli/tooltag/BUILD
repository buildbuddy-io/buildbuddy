load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tooltag",
    srcs = ["tooltag.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/tooltag",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/version",
    ],
)

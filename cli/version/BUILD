load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "version",
    srcs = ["version.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/version",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//server/version",
    ],
)

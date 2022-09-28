load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "parser",
    srcs = ["parser.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/parser",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/logging",
    ],
)

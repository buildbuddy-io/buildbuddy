load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "login",
    srcs = ["login.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/cli/login",
    visibility = ["//visibility:public"],
    deps = [
        "//cli/arg",
        "//cli/log",
        "//cli/storage",
    ],
)

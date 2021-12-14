load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "statusz",
    srcs = ["statusz.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/statusz",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/tracing/filepath",
        "//server/util/tracing/os",
        "//server/version",
    ],
)

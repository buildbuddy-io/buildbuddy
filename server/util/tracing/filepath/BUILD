load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "filepath",
    srcs = ["filepath.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/tracing/filepath",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/tracing/fs",
        "//server/util/tracing/os",
        "//server/util/tracing/span",
    ],
)

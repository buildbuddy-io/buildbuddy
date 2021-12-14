load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "disk",
    srcs = ["disk.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/disk",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//server/util/random",
        "//server/util/status",
        "//server/util/tracing/ctxio",
        "//server/util/tracing/filepath",
        "//server/util/tracing/fs",
        "//server/util/tracing/os",
    ],
)

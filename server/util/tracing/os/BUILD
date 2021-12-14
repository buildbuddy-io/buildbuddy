load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "os",
    srcs = ["os.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/tracing/os",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/tracing/fs",
        "//server/util/tracing/span",
    ],
)

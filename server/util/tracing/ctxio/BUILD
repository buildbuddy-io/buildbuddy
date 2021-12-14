load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "ctxio",
    srcs = ["ctxio.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/tracing/ctxio",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/tracing/span",
    ],
)

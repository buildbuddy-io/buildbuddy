load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "prefix",
    srcs = ["prefix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/prefix",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
        "//server/util/status",
    ],
)

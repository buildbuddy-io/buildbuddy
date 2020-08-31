load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["prefix.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/prefix",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment:go_default_library",
        "//server/util/status:go_default_library",
    ],
)

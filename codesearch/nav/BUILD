load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "nav",
    srcs = ["lookup.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/nav",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/annotations",
        "//codesearch/index",
        "//codesearch/schema",
    ],
)

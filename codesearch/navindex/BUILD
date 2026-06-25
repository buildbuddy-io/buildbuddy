load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "navindex",
    srcs = ["navindex.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/navindex",
    visibility = ["//visibility:public"],
    deps = [
        "//codesearch/annotations",
        "//codesearch/index",
        "//codesearch/query",
        "//codesearch/schema",
    ],
)

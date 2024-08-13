load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "dfa",
    srcs = [
        "copy.go",
        "dfa.go",
        "regexp.go",
        "utf.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/codesearch/dfa",
    visibility = ["//visibility:public"],
    deps = ["//codesearch/sparse"],
)

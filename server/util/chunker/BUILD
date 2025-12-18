load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "chunker",
    srcs = ["chunker.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/chunker",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/status",
        "@com_github_jotfs_fastcdc_go//:fastcdc-go",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "blobstore",
    srcs = ["blobstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore",
    visibility = ["//visibility:public"],
    deps = [
        "//server/backends/blobstore/aws",
        "//server/backends/blobstore/azure",
        "//server/backends/blobstore/disk",
        "//server/backends/blobstore/gcs",
        "//server/environment",
        "//server/interfaces",
        "//server/util/log",
    ],
)

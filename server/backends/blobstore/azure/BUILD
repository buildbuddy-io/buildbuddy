load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "azure",
    srcs = ["azure.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/azure",
    visibility = ["//visibility:public"],
    deps = [
        "//server/backends/blobstore/util",
        "//server/interfaces",
        "//server/util/flagutil",
        "//server/util/ioutil",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_azure_azure_storage_blob_go//azblob",
    ],
)

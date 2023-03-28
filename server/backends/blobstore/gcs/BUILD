load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "gcs",
    srcs = ["gcs.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs",
    visibility = ["//visibility:public"],
    deps = [
        "//server/backends/blobstore/util",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_google_cloud_go_storage//:storage",
        "@org_golang_google_api//option",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["gcs_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/gcs_cache",
    visibility = ["//visibility:public"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/status:go_default_library",
        "@com_google_cloud_go_storage//:go_default_library",
        "@org_golang_google_api//googleapi:go_default_library",
        "@org_golang_google_api//option:go_default_library",
        "@org_golang_x_sync//errgroup:go_default_library",
    ],
)

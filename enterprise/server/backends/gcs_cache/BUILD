load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "gcs_cache",
    srcs = ["gcs_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/gcs_cache",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/cache_metrics",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "//server/util/tracing",
        "@com_google_cloud_go_storage//:storage",
        "@org_golang_google_api//googleapi",
        "@org_golang_google_api//option",
        "@org_golang_x_sync//errgroup",
    ],
)

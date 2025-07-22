load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "errors_lib",
    srcs = ["errors.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/gcs/errors",
    visibility = ["//visibility:private"],
    deps = [
        "@com_github_googleapis_gax_go_v2//apierror",
        "@com_google_cloud_go_storage//:storage",
        "@org_golang_google_grpc//status",
    ],
)

go_binary(
    name = "errors",
    embed = [":errors_lib"],
    visibility = ["//visibility:public"],
)

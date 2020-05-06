load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["blobstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore",
    visibility = ["//visibility:public"],
    deps = [
        "//server/config:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/util/disk:go_default_library",
        "@com_github_aws_aws_sdk_go//aws:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/awserr:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/credentials:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/session:go_default_library",
        "@com_github_aws_aws_sdk_go//service/s3:go_default_library",
        "@com_github_aws_aws_sdk_go//service/s3/s3manager:go_default_library",
        "@com_google_cloud_go_storage//:go_default_library",
        "@org_golang_google_api//option:go_default_library",
    ],
)

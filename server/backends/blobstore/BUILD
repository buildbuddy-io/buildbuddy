load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "blobstore",
    srcs = ["blobstore.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/disk",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_aws_aws_sdk_go//aws",
        "@com_github_aws_aws_sdk_go//aws/awserr",
        "@com_github_aws_aws_sdk_go//aws/credentials",
        "@com_github_aws_aws_sdk_go//aws/credentials/stscreds",
        "@com_github_aws_aws_sdk_go//aws/session",
        "@com_github_aws_aws_sdk_go//service/s3",
        "@com_github_aws_aws_sdk_go//service/s3/s3manager",
        "@com_github_aws_aws_sdk_go//service/sts",
        "@com_github_azure_azure_storage_blob_go//azblob",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_google_cloud_go_storage//:storage",
        "@org_golang_google_api//option",
        "@org_golang_google_grpc//status",
    ],
)

go_test(
    name = "blobstore_test",
    srcs = ["blobstore_test.go"],
    embed = [":blobstore"],
    deps = ["@com_github_stretchr_testify//require"],
)

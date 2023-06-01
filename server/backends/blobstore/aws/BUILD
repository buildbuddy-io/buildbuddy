load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "aws",
    srcs = ["aws.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/backends/blobstore/aws",
    visibility = ["//visibility:public"],
    deps = [
        "//server/backends/blobstore/util",
        "//server/interfaces",
        "//server/util/flagutil",
        "//server/util/ioutil",
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
    ],
)

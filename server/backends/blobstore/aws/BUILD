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
        "@com_github_aws_aws_sdk_go_v2//aws",
        "@com_github_aws_aws_sdk_go_v2_config//:config",
        "@com_github_aws_aws_sdk_go_v2_credentials//:credentials",
        "@com_github_aws_aws_sdk_go_v2_credentials//stscreds",
        "@com_github_aws_aws_sdk_go_v2_feature_s3_manager//:manager",
        "@com_github_aws_aws_sdk_go_v2_service_s3//:s3",
        "@com_github_aws_aws_sdk_go_v2_service_sts//:sts",
        "@com_github_aws_smithy_go//:smithy-go",
    ],
)

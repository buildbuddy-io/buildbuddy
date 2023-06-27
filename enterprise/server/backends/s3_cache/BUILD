load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "s3_cache",
    srcs = ["s3_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache",
    deps = [
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/cache_metrics",
        "//server/util/flagutil",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "//server/util/tracing",
        "@com_github_aws_aws_sdk_go_v2//aws",
        "@com_github_aws_aws_sdk_go_v2_config//:config",
        "@com_github_aws_aws_sdk_go_v2_credentials//:credentials",
        "@com_github_aws_aws_sdk_go_v2_credentials//stscreds",
        "@com_github_aws_aws_sdk_go_v2_feature_s3_manager//:manager",
        "@com_github_aws_aws_sdk_go_v2_service_s3//:s3",
        "@com_github_aws_aws_sdk_go_v2_service_s3//types",
        "@com_github_aws_aws_sdk_go_v2_service_sts//:sts",
        "@com_github_aws_smithy_go//:smithy-go",
        "@org_golang_x_sync//errgroup",
    ],
)

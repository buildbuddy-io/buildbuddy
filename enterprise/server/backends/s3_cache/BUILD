load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "s3_cache",
    srcs = ["s3_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache",
    visibility = [
        "//enterprise:__subpackages__",
        "@buildbuddy_internal//enterprise:__subpackages__",
    ],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/config",
        "//server/interfaces",
        "//server/remote_cache/digest",
        "//server/util/cache_metrics",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_aws_aws_sdk_go//aws",
        "@com_github_aws_aws_sdk_go//aws/awserr",
        "@com_github_aws_aws_sdk_go//aws/credentials",
        "@com_github_aws_aws_sdk_go//aws/session",
        "@com_github_aws_aws_sdk_go//service/s3",
        "@com_github_aws_aws_sdk_go//service/s3/s3manager",
        "@org_golang_x_sync//errgroup",
    ],
)

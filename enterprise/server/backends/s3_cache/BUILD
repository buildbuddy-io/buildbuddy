load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "go_default_library",
    srcs = ["s3_cache.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/s3_cache",
    visibility = ["//enterprise:__subpackages__"],
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/config:go_default_library",
        "//server/interfaces:go_default_library",
        "//server/remote_cache/digest:go_default_library",
        "//server/util/cache_metrics:go_default_library",
        "//server/util/prefix:go_default_library",
        "//server/util/status:go_default_library",
        "@com_github_aws_aws_sdk_go//aws:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/awserr:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/credentials:go_default_library",
        "@com_github_aws_aws_sdk_go//aws/session:go_default_library",
        "@com_github_aws_aws_sdk_go//service/s3:go_default_library",
        "@com_github_aws_aws_sdk_go//service/s3/s3manager:go_default_library",
        "@org_golang_x_sync//errgroup:go_default_library",
    ],
)

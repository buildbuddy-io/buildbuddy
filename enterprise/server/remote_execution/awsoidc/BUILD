load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "awsoidc",
    srcs = ["awsoidc.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/awsoidc",
    deps = [
        "//proto:remote_execution_go_proto",
        "//server/util/status",
        "@com_github_aws_aws_sdk_go//aws",
        "@com_github_aws_aws_sdk_go//aws/credentials",
        "@com_github_aws_aws_sdk_go//aws/session",
        "@com_github_aws_aws_sdk_go//service/ecr",
        "@com_github_aws_aws_sdk_go//service/sts",
    ],
)

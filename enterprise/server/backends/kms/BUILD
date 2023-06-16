load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "kms",
    srcs = ["kms.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms",
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_aws_aws_sdk_go//aws",
        "@com_github_aws_aws_sdk_go//aws/credentials",
        "@com_github_aws_aws_sdk_go//aws/session",
        "@com_github_aws_aws_sdk_go//service/kms",
        "@com_github_google_tink_go//core/registry",
        "@com_github_google_tink_go//integration/awskms",
        "@com_github_google_tink_go//integration/gcpkms",
        "@com_github_google_tink_go//tink",
        "@org_golang_google_api//option",
    ],
)

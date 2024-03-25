load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "kms",
    srcs = ["kms.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms",
    deps = [
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_aws_aws_sdk_go_v2//aws",
        "@com_github_aws_aws_sdk_go_v2_config//:config",
        "@com_github_aws_aws_sdk_go_v2_credentials//:credentials",
        "@com_github_aws_aws_sdk_go_v2_service_kms//:kms",
        "@com_github_tink_crypto_tink_go_awskms_v2//integration/awskms",
        "@com_github_tink_crypto_tink_go_gcpkms_v2//integration/gcpkms",
        "@com_github_tink_crypto_tink_go_v2//core/registry",
        "@com_github_tink_crypto_tink_go_v2//tink",
        "@org_golang_google_api//option",
    ],
)

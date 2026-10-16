load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

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
        "@com_github_aws_aws_sdk_go//aws",
        "@com_github_aws_aws_sdk_go//aws/credentials",
        "@com_github_aws_aws_sdk_go//aws/session",
        "@com_github_aws_aws_sdk_go//service/kms",
        "@com_github_tink_crypto_tink_go_awskms_v2//integration/awskms",
        "@com_github_tink_crypto_tink_go_gcpkms_v2//integration/gcpkms",
        "@com_github_tink_crypto_tink_go_v2//core/registry",
        "@com_github_tink_crypto_tink_go_v2//tink",
        "@org_golang_google_api//cloudkms/v1:cloudkms",
        "@org_golang_google_api//option",
    ],
)

go_test(
    name = "kms_test",
    srcs = ["kms_test.go"],
    deps = [
        ":kms",
        "//server/testutil/testfs",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "testencryption",
    testonly = 1,
    srcs = ["testencryption.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/testencryption",
    deps = [
        "//enterprise/server/backends/kms",
        "//enterprise/server/crypter_service",
        "//proto:encryption_go_proto",
        "//server/environment",
        "//server/real_environment",
        "//server/testutil/testfs",
        "//server/util/claims",
        "//server/util/flagutil",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

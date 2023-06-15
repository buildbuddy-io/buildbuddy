load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "enterprise_testauth",
    testonly = 1,
    srcs = ["enterprise_testauth.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth",
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/tables",
        "//server/testutil/testauth",
        "//server/util/claims",
        "//server/util/request_context",
        "@com_github_stretchr_testify//require",
    ],
)

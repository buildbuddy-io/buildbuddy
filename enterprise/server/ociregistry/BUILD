load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ociregistry",
    srcs = ["ociregistry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/prefix",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
    ],
)

go_test(
    name = "ociregistry_test",
    timeout = "short",
    srcs = ["ociregistry_test.go"],
    deps = [
        ":ociregistry",
        "//server/environment",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/testutil/testregistry",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

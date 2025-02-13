load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ociregistry",
    srcs = ["ociregistry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/ociregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/metrics",
        "//server/real_environment",
        "//server/util/log",
        "//server/util/prefix",
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
        "@com_github_google_go_containerregistry//pkg/v1/types",
    ],
)

go_test(
    name = "ociregistry_test",
    timeout = "short",
    srcs = ["ociregistry_test.go"],
    deps = [
        ":ociregistry",
        "//enterprise/server/util/oci",
        "//proto:registry_go_proto",
        "//server/environment",
        "//server/testutil/testenv",
        "//server/testutil/testport",
        "//server/testutil/testregistry",
        "//server/util/testing/flags",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

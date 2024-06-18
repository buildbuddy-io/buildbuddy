load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "oci",
    srcs = ["oci.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci",
    deps = [
        "//enterprise/server/remote_execution/platform",
        "//proto:registry_go_proto",
        "//server/util/flag",
        "//server/util/log",
        "//server/util/status",
        "@com_github_docker_distribution//reference",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/match",
        "@com_github_google_go_containerregistry//pkg/v1/partial",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/remote/transport",
        "@com_github_google_go_containerregistry//pkg/v1/types",
    ],
)

go_test(
    name = "oci_test",
    size = "small",
    timeout = "moderate",
    srcs = ["oci_test.go"],
    deps = [
        ":oci",
        "//enterprise/server/remote_execution/platform",
        "//proto:registry_go_proto",
        "//server/testutil/testregistry",
        "//server/util/proto",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

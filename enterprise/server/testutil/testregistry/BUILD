load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testregistry",
    testonly = 1,
    srcs = ["testregistry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//server/testutil/testdigest",
        "//server/testutil/testport",
        "//server/util/basicauth",
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/crane",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/registry",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/empty",
        "@com_github_google_go_containerregistry//pkg/v1/layout",
        "@com_github_google_go_containerregistry//pkg/v1/mutate",
        "@com_github_google_go_containerregistry//pkg/v1/partial",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/tarball",
        "@com_github_google_go_containerregistry//pkg/v1/types",
        "@com_github_stretchr_testify//require",
        "@io_bazel_rules_go//go/runfiles",
    ],
)

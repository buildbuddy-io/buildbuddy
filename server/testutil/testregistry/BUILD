load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "testregistry",
    testonly = 1,
    srcs = ["testregistry.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry",
    visibility = ["//visibility:public"],
    deps = [
        "//server/testutil/testport",
        "@com_github_google_go_containerregistry//pkg/crane",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/registry",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_stretchr_testify//require",
    ],
)

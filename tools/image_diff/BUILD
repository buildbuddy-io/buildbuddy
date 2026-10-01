load("@io_bazel_rules_go//go:def.bzl", "go_binary", "go_library")

go_library(
    name = "image_diff_lib",
    srcs = ["image_diff.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/image_diff",
    visibility = ["//visibility:private"],
    deps = [
        "@com_github_google_go_containerregistry//pkg/authn",
        "@com_github_google_go_containerregistry//pkg/name",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/layout",
        "@com_github_google_go_containerregistry//pkg/v1/mutate",
        "@com_github_google_go_containerregistry//pkg/v1/remote",
        "@com_github_google_go_containerregistry//pkg/v1/tarball",
    ],
)

go_binary(
    name = "image_diff",
    embed = [":image_diff_lib"],
    visibility = ["//visibility:public"],
)

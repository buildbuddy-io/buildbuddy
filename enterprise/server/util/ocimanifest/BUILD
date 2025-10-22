load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ocimanifest",
    srcs = ["ocimanifest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocimanifest",
    deps = [
        "//server/util/status",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "registry",
    srcs = ["and.go", "blobs.go", "browser.go", "error.go", "manifest.go", "registry.go", "verify.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/registry",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/interfaces",
        "//server/util/hash",
        "//server/util/prefix",
        "//proto:remote_execution_go_proto",
        "//proto:resource_go_proto",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_google_go_containerregistry//pkg/v1/types",
    ],
)

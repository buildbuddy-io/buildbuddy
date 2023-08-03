load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //enterprise:__subpackages__
package(default_visibility = ["//enterprise:__subpackages__"])

filegroup(
    name = "licenses",
    srcs = [
        "LICENSE",
    ],
    visibility = ["//visibility:public"],
)

# gazelle:ignore
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [":licenses"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise",
    deps = [
        "//server/util/fileresolver",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

# gazelle:default_visibility //visibility:public
package(default_visibility = ["//visibility:public"])

filegroup(
    name = "config_files",
    srcs = select({
        "//:release_build": ["buildbuddy.release.yaml"],
        "//conditions:default": glob(["**"]),
    }),
)

# gazelle:ignore
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [":config_files"],
    importpath = "github.com/buildbuddy-io/buildbuddy/config",
    deps = [
        "//server/util/fileresolver",
    ],
)

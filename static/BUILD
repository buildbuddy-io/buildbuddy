load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "static",
    srcs = glob([
        "favicon/**",
        "image/**",
        "favicon.ico",
        "index.html",
    ]),
)

# gazelle:ignore
go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [":static"],
    importpath = "github.com/buildbuddy-io/buildbuddy/v2/static",
    deps = [
        "//server/util/fileresolver",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//visibility:public"])

filegroup(
    name = "static",
    srcs = [
        "auto_refresh.js",
        "favicon.ico",
        "index.html",
    ] + glob([
        "favicon/**",
        "image/**",
    ]),
)

go_library(
    name = "bundle",
    srcs = ["bundle.go"],
    embedsrcs = [":static"],
    importpath = "github.com/buildbuddy-io/buildbuddy/static",
    deps = [
        "//server/util/fileresolver",
    ],
)

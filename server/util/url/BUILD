load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "url",
    srcs = ["url.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/url",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/status",
    ],
)
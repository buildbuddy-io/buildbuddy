load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "config",
    srcs = ["config.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/config",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flagutil",
        "//server/util/log",
        "@in_gopkg_yaml_v2//:yaml_v2",
    ],
)

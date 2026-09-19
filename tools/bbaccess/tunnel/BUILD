load("@io_bazel_rules_go//go:def.bzl", "go_library")

go_library(
    name = "tunnel",
    srcs = ["tunnel.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//tools/bbaccess/tunnel/credentials",
        "//tools/bbaccess/tunnel/daemon",
        "//tools/bbaccess/tunnel/install",
        "//tools/bbaccess/tunnel/tunnelconfig",
    ],
)

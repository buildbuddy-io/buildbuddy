load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "tunnel",
    srcs = ["tunnel.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flag",
        "//server/util/log",
        "//tools/bbaccess/tunnel/config",
        "//tools/bbaccess/tunnel/credentials",
        "//tools/bbaccess/tunnel/daemon",
        "//tools/bbaccess/tunnel/install",
    ],
)

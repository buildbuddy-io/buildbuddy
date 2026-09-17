load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "dnsserver",
    srcs = ["dnsserver.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/dnsserver",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//tools/bbaccess/tunnel/fakeip",
        "//tools/bbaccess/tunnel/tunnelconfig",
        "@com_github_miekg_dns//:dns",
    ],
)

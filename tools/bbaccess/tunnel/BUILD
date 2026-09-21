load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "tunnel",
    srcs = [
        "background.go",
        "background_other.go",
        "background_unix.go",
        "tunnel.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//tools/bbaccess/tunnel/credentials",
        "//tools/bbaccess/tunnel/daemon",
        "//tools/bbaccess/tunnel/install",
        "//tools/bbaccess/tunnel/tunnelconfig",
        "//tools/bbaccess/update",
    ],
)

go_test(
    name = "tunnel_test",
    srcs = [
        "background_test.go",
        "background_unix_test.go",
    ],
    embed = [":tunnel"],
    deps = ["@com_github_stretchr_testify//require"],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//cli:__subpackages__"])

go_library(
    name = "daemon",
    srcs = ["daemon.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/tools/bbaccess/tunnel/daemon",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/log",
        "//tools/bbaccess/tunnel/config",
        "//tools/bbaccess/tunnel/dnsserver",
        "//tools/bbaccess/tunnel/fakeip",
        "//tools/bbaccess/tunnel/interceptor",
        "//tools/bbaccess/tunnel/tundev",
        "//tools/bbaccess/tunnel/tunnelmgr",
    ],
)

go_test(
    name = "daemon_test",
    srcs = ["daemon_test.go"],
    embed = [":daemon"],
    deps = [
        "//tools/bbaccess/tunnel/config",
        "@com_github_stretchr_testify//require",
    ],
)

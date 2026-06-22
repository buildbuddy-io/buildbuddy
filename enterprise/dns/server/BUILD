load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "server",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/dns/server",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/log",
        "@com_github_miekg_dns//:dns",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "server_test",
    size = "small",
    srcs = ["server_test.go"],
    deps = [
        ":server",
        "@com_github_miekg_dns//:dns",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "server",
    srcs = ["server.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/dns/server",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/experiments",
        "//server/environment",
        "//server/metrics",
        "//server/util/log",
        "//server/util/maxmind",
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
        "//enterprise/server/experiments",
        "//server/testutil/testenv",
        "//server/util/maxmind",
        "@com_github_miekg_dns//:dns",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk_contrib_providers_flagd//pkg",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

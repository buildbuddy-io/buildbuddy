load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "server",
    srcs = [
        "challenges.go",
        "server.go",
        "watcher.go",
    ],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/dns/server",
    visibility = ["//visibility:public"],
    deps = [
        "//enterprise/server/experiments",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/util/alert",
        "//server/util/lib/set",
        "//server/util/log",
        "//server/util/lru",
        "//server/util/maxmind",
        "//server/util/status",
        "@com_github_miekg_dns//:dns",
        "@com_github_prometheus_client_golang//prometheus",
        "@org_golang_x_sync//singleflight",
    ],
)

go_test(
    name = "server_test",
    size = "small",
    srcs = [
        "server_test.go",
        "watcher_test.go",
    ],
    embed = [":server"],
    deps = [
        "//enterprise/server/experiments",
        "//server/interfaces",
        "//server/testutil/testenv",
        "//server/util/maxmind",
        "//server/util/status",
        "@com_github_miekg_dns//:dns",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk_contrib_providers_flagd//pkg",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

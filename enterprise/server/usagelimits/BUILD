load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "usagelimits",
    srcs = ["usagelimits.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/usagelimits",
    deps = [
        "//enterprise/server/experiments",
        "//enterprise/server/usage_service",
        "//proto:usage_go_proto",
        "//server/environment",
        "//server/interfaces",
        "//server/metrics",
        "//server/real_environment",
        "//server/util/authutil",
        "//server/util/claims",
        "//server/util/log",
        "//server/util/status",
        "@com_github_go_redis_redis_v8//:redis",
        "@com_github_prometheus_client_golang//prometheus",
    ],
)

go_test(
    name = "usagelimits_test",
    size = "small",
    srcs = ["usagelimits_test.go"],
    embed = [":usagelimits"],
    deps = [
        "//enterprise/server/experiments",
        "//enterprise/server/usage_service",
        "//proto:usage_go_proto",
        "//server/backends/memory_metrics_collector",
        "//server/environment",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testenv",
        "//server/usage/sku",
        "//server/util/status",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk//openfeature/memprovider",
        "@com_github_open_feature_go_sdk_contrib_providers_flagd//pkg",
        "@com_github_stretchr_testify//assert",
        "@com_github_stretchr_testify//require",
    ],
)

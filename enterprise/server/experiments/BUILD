load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "experiments",
    srcs = ["experiments.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/bazel_request",
        "//server/util/claims",
        "//server/util/flag",
        "//server/util/statusz",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk_contrib_providers_flagd//pkg",
    ],
)

package(default_visibility = ["//enterprise:__subpackages__"])

go_test(
    name = "experiments_test",
    srcs = ["experiments_test.go"],
    deps = [
        ":experiments",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/util/authutil",
        "//server/util/claims",
        "@com_github_open_feature_go_sdk//openfeature",
        "@com_github_open_feature_go_sdk//openfeature/memprovider",
        "@com_github_open_feature_go_sdk//openfeature/testing",
        "@com_github_open_feature_go_sdk_contrib_providers_flagd//pkg",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "pricing",
    srcs = ["pricing.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/pricing",
    visibility = ["//visibility:public"],
    deps = [
        "//server/interfaces",
        "//server/tables",
        "//server/usage/sku",
        "//server/util/db",
    ],
)

go_test(
    name = "pricing_test",
    srcs = ["pricing_test.go"],
    deps = [
        ":pricing",
        "//server/tables",
        "//server/testutil/testenv",
        "//server/usage/sku",
        "@com_github_stretchr_testify//require",
    ],
)

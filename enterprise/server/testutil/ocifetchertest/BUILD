load("@io_bazel_rules_go//go:def.bzl", "go_library")

package(default_visibility = ["//enterprise:__subpackages__"])

go_library(
    name = "ocifetchertest",
    testonly = 1,
    srcs = ["ocifetchertest.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/ocifetchertest",
    deps = [
        "//enterprise/server/testutil/testregistry",
        "//proto:capability_go_proto",
        "//proto:oci_fetcher_go_proto",
        "//proto:registry_go_proto",
        "//server/interfaces",
        "//server/testutil/testauth",
        "//server/testutil/testhttp",
        "//server/util/claims",
        "//server/util/status",
        "//server/util/testing/flags",
        "@com_github_google_go_containerregistry//pkg/v1:pkg",
        "@com_github_stretchr_testify//require",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "build_buddy_url",
    srcs = ["build_buddy_url.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url",
    visibility = ["//visibility:public"],
    deps = [
        "//server/util/flag",
        "//server/util/status",
        "//server/util/urlutil",
    ],
)

go_test(
    name = "build_buddy_url_test",
    srcs = ["build_buddy_url_test.go"],
    deps = [
        ":build_buddy_url",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

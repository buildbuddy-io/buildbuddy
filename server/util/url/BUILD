load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "url",
    srcs = ["url.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/url",
    visibility = ["//visibility:public"],
    deps = [
        "//server/endpoint_urls/build_buddy_url",
        "//server/environment",
        "//server/util/status",
    ],
)

go_test(
    name = "url_test",
    size = "small",
    srcs = ["url_test.go"],
    deps = [
        ":url",
        "//server/environment",
        "//server/real_environment",
        "//server/util/healthcheck",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

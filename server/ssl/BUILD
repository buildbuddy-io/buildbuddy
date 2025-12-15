load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "ssl",
    srcs = ["ssl.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/ssl",
    visibility = ["//visibility:public"],
    deps = [
        "//server/endpoint_urls/build_buddy_url",
        "//server/endpoint_urls/cache_api_url",
        "//server/endpoint_urls/events_api_url",
        "//server/environment",
        "//server/interfaces",
        "//server/real_environment",
        "//server/util/flag",
        "//server/util/status",
        "@org_golang_google_grpc//credentials",
        "@org_golang_x_crypto//acme",
        "@org_golang_x_crypto//acme/autocert",
    ],
)

go_test(
    name = "ssl_test",
    srcs = ["ssl_test.go"],
    deps = [
        ":ssl",
        "//server/testutil/testenv",
        "//server/util/testing/flags",
        "@com_github_stretchr_testify//require",
    ],
)

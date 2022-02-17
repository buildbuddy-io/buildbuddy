load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "url",
    srcs = ["url.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/url",
    visibility = ["//visibility:public"],
    deps = [
        "//server/environment",
        "//server/util/log",
        "//server/util/status",
    ],
)

go_test(
    name = "url_test",
    srcs = ["url_test.go"],
    deps = [
        ":url",
        "//server/config",
        "//server/environment",
        "//server/real_environment",
        "//server/util/healthcheck",
    ],
)

load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")

go_library(
    name = "compression",
    srcs = ["compression.go"],
    importpath = "github.com/buildbuddy-io/buildbuddy/server/util/compression",
    visibility = ["//visibility:public"],
    deps = [
        "//server/metrics",
        "//server/util/log",
        "@com_github_prometheus_client_golang//prometheus",
        "@com_github_valyala_gozstd//:gozstd",
    ],
)

go_test(
    name = "compression_test",
    srcs = ["compression_test.go"],
    embed = [":compression"],
    deps = ["@com_github_stretchr_testify//require"],
)
